package extension

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const maxReportedFailures = 64

// Registry owns the complete lifecycle and immutable payload of mounted
// components. T is supplied by the production host composing those payloads.
type Registry[T any] struct {
	mu       sync.Mutex
	reporter Reporter
	mounts   map[string]*mountState[T]
}

func NewRegistry[T any](reporter Reporter) *Registry[T] {
	return &Registry[T]{reporter: reporter, mounts: make(map[string]*mountState[T])}
}

type cleanupState struct {
	fn   Cleanup
	done bool
}

type callbackToken struct{}

type mountState[T any] struct {
	component       Component
	entries         []registrationEntry
	selectionScopes []Scope
	value           T
	effects         []cleanupState
	active          bool
	refs            int
	drained         chan struct{}
	token           *callbackToken
	cleanMu         sync.Mutex
}

type stagingRegistrar struct {
	component Component
	entries   []registrationEntry
	effects   []cleanupState
	closed    bool
}

func (s *stagingRegistrar) register(entry registrationEntry) error {
	if s.closed {
		return ErrMountClosed
	}
	if !identifierPattern.MatchString(entry.spec.ID) {
		return fmt.Errorf("%w: registration identity is invalid", ErrInvalidRegistration)
	}
	if err := ValidateScope(entry.spec.Scope); err != nil {
		return err
	}
	for _, existing := range s.entries {
		if existing.point == entry.point && existing.spec.Scope == entry.spec.Scope && existing.spec.ID == entry.spec.ID {
			return fmt.Errorf("%w: %s", ErrDuplicateRegistration, entry.spec.ID)
		}
	}
	s.entries = append(s.entries, entry)
	return nil
}

func (s *stagingRegistrar) InstanceID() string { return s.component.InstanceID }

func (s *stagingRegistrar) Defer(cleanup Cleanup) error {
	if s.closed {
		return ErrMountClosed
	}
	if cleanup == nil {
		return fmt.Errorf("%w: nil cleanup", ErrInvalidRegistration)
	}
	s.effects = append(s.effects, cleanupState{fn: cleanup})
	return nil
}

// PreparedMount contains validated registrations and cleanup effects that have
// not yet been published.
type PreparedMount[T any] struct {
	mu         sync.Mutex
	registry   *Registry[T]
	state      *mountState[T]
	committed  bool
	rolledBack bool
}

func (r *Registry[T]) PrepareMount(ctx context.Context, component Component, installer Installer) (prepared *PreparedMount[T], err error) {
	if r == nil || installer == nil {
		return nil, fmt.Errorf("%w: registry and installer required", ErrInvalidComponent)
	}
	if err := ValidateComponent(component); err != nil {
		return nil, err
	}
	stage := &stagingRegistrar{component: component}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension installer panic: %v", recovered)
		}
		if err != nil {
			stage.closed = true
			err = errors.Join(err, cleanupReverse(context.WithoutCancel(ctx), stage.effects))
		}
	}()
	if err = installer.Install(ctx, stage); err != nil {
		return nil, err
	}
	stage.closed = true
	state := &mountState[T]{
		component: component,
		entries:   append([]registrationEntry(nil), stage.entries...),
		effects:   append([]cleanupState(nil), stage.effects...),
		drained:   make(chan struct{}),
		token:     &callbackToken{},
	}
	return &PreparedMount[T]{registry: r, state: state}, nil
}

// CommitValue is an immutable validator-only view of one component payload.
// Validators run synchronously under the publication lock and must not retain
// these values.
type CommitValue[T any] struct {
	component Component
	value     T
}

func (v CommitValue[T]) Component() Component { return v.component }
func (v CommitValue[T]) Value() T             { return v.value }

type CommitValidator[T any] func(active []CommitValue[T], candidate CommitValue[T]) error

// CommitMount atomically validates and publishes a prepared mount. A failed
// commit leaves the preparation available for Rollback.
func (r *Registry[T]) CommitMount(prepared *PreparedMount[T], value T, selectionScopes []Scope, validate CommitValidator[T]) (*Mount[T], error) {
	if r == nil || prepared == nil {
		return nil, fmt.Errorf("%w: registry and prepared mount required", ErrInvalidComponent)
	}
	scopes, err := freezeScopes(selectionScopes)
	if err != nil {
		return nil, err
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.registry != r || prepared.state == nil || prepared.committed || prepared.rolledBack {
		return nil, ErrMountClosed
	}
	state := prepared.state
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mounts == nil {
		r.mounts = make(map[string]*mountState[T])
	}
	if _, exists := r.mounts[state.component.InstanceID]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateInstance, state.component.InstanceID)
	}
	candidate := CommitValue[T]{component: state.component, value: value}
	if validate != nil {
		active := make([]CommitValue[T], 0, len(r.mounts))
		for _, mounted := range r.mounts {
			if mounted.active {
				active = append(active, CommitValue[T]{component: mounted.component, value: mounted.value})
			}
		}
		if err := callCommitValidator(validate, active, candidate); err != nil {
			return nil, err
		}
	}
	state.value = value
	state.selectionScopes = scopes
	state.active = true
	r.mounts[state.component.InstanceID] = state
	prepared.committed = true
	return &Mount[T]{registry: r, state: state}, nil
}

func callCommitValidator[T any](validate CommitValidator[T], active []CommitValue[T], candidate CommitValue[T]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension commit validator panic: %v", recovered)
		}
	}()
	return validate(active, candidate)
}

func freezeScopes(scopes []Scope) ([]Scope, error) {
	result := make([]Scope, 0, len(scopes))
	seen := make(map[Scope]bool, len(scopes))
	for _, scope := range scopes {
		if err := ValidateScope(scope); err != nil {
			return nil, err
		}
		if !seen[scope] {
			seen[scope] = true
			result = append(result, scope)
		}
	}
	return result, nil
}

// Rollback discards an uncommitted preparation and runs cleanup effects in
// reverse order. It is idempotent and cleanup cannot inherit cancellation.
func (p *PreparedMount[T]) Rollback(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.committed || p.rolledBack {
		p.mu.Unlock()
		return nil
	}
	p.rolledBack = true
	effects := p.state.effects
	p.mu.Unlock()
	return cleanupReverse(context.WithoutCancel(ctx), effects)
}

type Mount[T any] struct {
	registry *Registry[T]
	state    *mountState[T]
}

func (m *Mount[T]) Deactivate() {
	if m == nil || m.registry == nil || m.state == nil {
		return
	}
	m.registry.mu.Lock()
	m.deactivateLocked()
	m.registry.mu.Unlock()
}

func (m *Mount[T]) deactivateLocked() {
	if !m.state.active {
		return
	}
	m.state.active = false
	delete(m.registry.mounts, m.state.component.InstanceID)
	if m.state.refs == 0 {
		close(m.state.drained)
	}
}

type callbackMountKey struct{}

func callbackContext(ctx context.Context, token *callbackToken) context.Context {
	if token == nil {
		return ctx
	}
	return context.WithValue(ctx, callbackMountKey{}, token)
}

// CallbackContext marks ctx as executing a callback owned by this mount.
func (m *Mount[T]) CallbackContext(ctx context.Context) context.Context {
	if m == nil || m.state == nil {
		return ctx
	}
	return callbackContext(ctx, m.state.token)
}

func (m *Mount[T]) CheckClose(ctx context.Context) error {
	if m == nil || m.state == nil {
		return nil
	}
	if token, _ := ctx.Value(callbackMountKey{}).(*callbackToken); token == m.state.token {
		return ErrSelfClose
	}
	return nil
}

func (m *Mount[T]) Close(ctx context.Context) error {
	if m == nil || m.registry == nil || m.state == nil {
		return nil
	}
	if err := m.CheckClose(ctx); err != nil {
		return err
	}
	m.registry.mu.Lock()
	m.deactivateLocked()
	drained := m.state.drained
	m.registry.mu.Unlock()
	select {
	case <-drained:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.state.cleanMu.Lock()
	defer m.state.cleanMu.Unlock()
	var errs []error
	for index := len(m.state.effects) - 1; index >= 0; index-- {
		effect := &m.state.effects[index]
		if effect.done {
			continue
		}
		if err := callCleanup(ctx, effect.fn); err != nil {
			if len(errs) < maxReportedFailures {
				errs = append(errs, err)
			}
			continue
		}
		effect.done = true
	}
	return errors.Join(errs...)
}

func cleanupReverse(ctx context.Context, effects []cleanupState) error {
	var errs []error
	for index := len(effects) - 1; index >= 0; index-- {
		if err := callCleanup(ctx, effects[index].fn); err != nil && len(errs) < maxReportedFailures {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func callCleanup(ctx context.Context, cleanup Cleanup) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension cleanup panic: %v", recovered)
		}
	}()
	return cleanup(ctx)
}

type Plan struct {
	entries  []plannedEntry
	reporter Reporter
	release  sync.Once
	releases []func()
}

type plannedEntry struct {
	registrationEntry
	component Component
	token     *callbackToken
}

// MountedValue is an immutable, lease-protected payload selected into a
// snapshot.
type MountedValue[T any] struct {
	component Component
	value     T
	token     *callbackToken
}

func (v MountedValue[T]) Component() Component { return v.component }
func (v MountedValue[T]) Value() T             { return v.value }
func (v MountedValue[T]) CallbackContext(ctx context.Context) context.Context {
	return callbackContext(ctx, v.token)
}

// Snapshot couples typed payloads to the exact dispatch plan that owns their
// canonical mount references.
type Snapshot[T any] struct {
	dispatch *Plan
	values   []MountedValue[T]
}

func (s *Snapshot[T]) Dispatch() *Plan {
	if s == nil {
		return nil
	}
	return s.dispatch
}

func (s *Snapshot[T]) Values() []MountedValue[T] {
	if s == nil {
		return nil
	}
	return append([]MountedValue[T](nil), s.values...)
}

func (s *Snapshot[T]) Release() {
	if s != nil && s.dispatch != nil {
		s.dispatch.Release()
	}
}

func (r *Registry[T]) Snapshot(target Scope) (*Snapshot[T], error) {
	return r.snapshot(target, nil)
}

// SnapshotInstances freezes only the named active mount instances.
func (r *Registry[T]) SnapshotInstances(target Scope, instanceIDs []string) (*Snapshot[T], error) {
	allowed := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		allowed[id] = true
	}
	return r.snapshot(target, allowed)
}

func (r *Registry[T]) snapshot(target Scope, allowed map[string]bool) (*Snapshot[T], error) {
	if r == nil {
		return &Snapshot[T]{dispatch: &Plan{}}, nil
	}
	if err := ValidateScope(target); err != nil {
		return nil, err
	}
	r.mu.Lock()
	entries := make([]plannedEntry, 0)
	leased := make(map[*mountState[T]]struct{})
	values := make([]MountedValue[T], 0)
	for _, state := range r.mounts {
		if !state.active || allowed != nil && !allowed[state.component.InstanceID] {
			continue
		}
		applies := false
		for _, entry := range state.entries {
			if !scopeApplies(entry.spec.Scope, target) {
				continue
			}
			entries = append(entries, plannedEntry{registrationEntry: entry, component: state.component, token: state.token})
			applies = true
		}
		for _, scope := range state.selectionScopes {
			if scopeApplies(scope, target) {
				applies = true
				break
			}
		}
		if applies {
			leased[state] = struct{}{}
			values = append(values, MountedValue[T]{component: state.component, value: state.value, token: state.token})
		}
	}
	releases := make([]func(), 0, len(leased))
	for state := range leased {
		state.refs++
		leasedState := state
		releases = append(releases, func() { r.release(leasedState) })
	}
	r.mu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entryLess(entries[i], entries[j]) })
	sort.Slice(values, func(i, j int) bool { return values[i].component.InstanceID < values[j].component.InstanceID })
	dispatch := &Plan{entries: entries, reporter: r.reporter, releases: releases}
	return &Snapshot[T]{dispatch: dispatch, values: values}, nil
}

// PlanEntryDiagnostic describes a callback identity frozen into a plan.
type PlanEntryDiagnostic struct {
	InstanceID string
	Artifact   Artifact
	ID         string
	Contract   Contract
	Order      int
	Scope      Scope
	Kind       string
}

// Diagnostics returns identities only; callback values are never exposed.
func (p *Plan) Diagnostics() []PlanEntryDiagnostic {
	if p == nil {
		return nil
	}
	result := make([]PlanEntryDiagnostic, 0, len(p.entries))
	for _, entry := range p.entries {
		kind := "notification"
		if entry.kind == entryInterceptor {
			kind = "interceptor"
		}
		result = append(result, PlanEntryDiagnostic{InstanceID: entry.component.InstanceID, Artifact: entry.component.Artifact, ID: entry.spec.ID, Contract: entry.contract, Order: entry.spec.Order, Scope: entry.spec.Scope, Kind: kind})
	}
	return result
}

func scopeApplies(registration, target Scope) bool {
	return registration.Kind == ScopeGlobal || registration.Kind == ScopeSession && target.Kind == ScopeSession && registration.Key == target.Key
}

func entryLess(left, right plannedEntry) bool {
	if left.spec.Order != right.spec.Order {
		return left.spec.Order < right.spec.Order
	}
	leftRank, rightRank := 0, 0
	if left.spec.Scope.Kind == ScopeSession {
		leftRank = 1
	}
	if right.spec.Scope.Kind == ScopeSession {
		rightRank = 1
	}
	if leftRank != rightRank {
		return leftRank < rightRank
	}
	if left.component.InstanceID != right.component.InstanceID {
		return left.component.InstanceID < right.component.InstanceID
	}
	return left.spec.ID < right.spec.ID
}

func (r *Registry[T]) release(state *mountState[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if state.refs > 0 {
		state.refs--
	}
	if state.refs == 0 && !state.active {
		select {
		case <-state.drained:
		default:
			close(state.drained)
		}
	}
}

func (p *Plan) Release() {
	if p == nil {
		return
	}
	p.release.Do(func() {
		for _, release := range p.releases {
			release()
		}
	})
}
