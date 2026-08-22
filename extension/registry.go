package extension

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

const maxReportedFailures = 64

type Registry struct {
	mu       sync.Mutex
	reporter Reporter
	mounts   map[string]*mountState
}

func NewRegistry(reporter Reporter) *Registry {
	return &Registry{reporter: reporter, mounts: make(map[string]*mountState)}
}

type cleanupState struct {
	fn   Cleanup
	done bool
}

type mountState struct {
	component Component
	entries   []registrationEntry
	effects   []cleanupState
	active    bool
	refs      int
	drained   chan struct{}
	cleanMu   sync.Mutex
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
	if entry.spec.InstanceID != s.component.InstanceID || !identifierPattern.MatchString(entry.spec.ID) {
		return fmt.Errorf("%w: registration identity does not match mount", ErrInvalidRegistration)
	}
	if err := validateScope(entry.spec.Scope); err != nil {
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

func (r *Registry) Mount(ctx context.Context, component Component, installer Installer) (*Mount, error) {
	prepared, err := r.PrepareMount(ctx, component, installer)
	if err != nil {
		return nil, err
	}
	mount, err := r.CommitMount(prepared)
	if err != nil {
		return nil, errors.Join(err, prepared.Rollback(context.WithoutCancel(ctx)))
	}
	return mount, nil
}

// PreparedMount contains validated registrations and cleanup effects that have
// not yet been published. Preparing executes installer code without holding a
// registry lock; CommitMount performs only the atomic publication step.
type PreparedMount struct {
	mu         sync.Mutex
	registry   *Registry
	state      *mountState
	committed  bool
	rolledBack bool
}

func (r *Registry) PrepareMount(ctx context.Context, component Component, installer Installer) (prepared *PreparedMount, err error) {
	if r == nil || installer == nil {
		return nil, fmt.Errorf("%w: registry and installer required", ErrInvalidComponent)
	}
	if err := validateArtifact(component); err != nil {
		return nil, err
	}
	stage := &stagingRegistrar{component: component}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("extension installer panic: %v", recovered)
		}
		if err != nil {
			stage.closed = true
			err = errors.Join(err, cleanupReverse(ctx, stage.effects))
		}
	}()
	if err = installer.Install(ctx, stage); err != nil {
		return nil, err
	}
	stage.closed = true
	state := &mountState{component: component, entries: append([]registrationEntry(nil), stage.entries...), effects: append([]cleanupState(nil), stage.effects...), drained: make(chan struct{})}
	return &PreparedMount{registry: r, state: state}, nil
}

// CommitMount atomically publishes a prepared mount. A failed commit leaves the
// preparation available for Rollback, which always runs cleanup outside locks.
func (r *Registry) CommitMount(prepared *PreparedMount) (*Mount, error) {
	if r == nil || prepared == nil {
		return nil, fmt.Errorf("%w: registry and prepared mount required", ErrInvalidComponent)
	}
	prepared.mu.Lock()
	defer prepared.mu.Unlock()
	if prepared.registry != r || prepared.state == nil || prepared.committed || prepared.rolledBack {
		return nil, ErrMountClosed
	}
	state := prepared.state
	r.mu.Lock()
	if r.mounts == nil {
		r.mounts = make(map[string]*mountState)
	}
	if _, exists := r.mounts[state.component.InstanceID]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrDuplicateInstance, state.component.InstanceID)
	}
	state.active = true
	r.mounts[state.component.InstanceID] = state
	r.mu.Unlock()
	prepared.committed = true
	return &Mount{registry: r, state: state}, nil
}

// Rollback discards an uncommitted preparation and runs its cleanup effects in
// reverse order. It is idempotent.
func (p *PreparedMount) Rollback(ctx context.Context) error {
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
	return cleanupReverse(ctx, effects)
}

type Mount struct {
	registry *Registry
	state    *mountState
}

func (m *Mount) Deactivate() {
	if m == nil || m.registry == nil || m.state == nil {
		return
	}
	m.registry.mu.Lock()
	m.deactivateLocked()
	m.registry.mu.Unlock()
}

func (m *Mount) deactivateLocked() {
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

// CallbackContext marks ctx as executing a callback owned by this mount.
func (m *Mount) CallbackContext(ctx context.Context) context.Context {
	if m == nil || m.state == nil {
		return ctx
	}
	return context.WithValue(ctx, callbackMountKey{}, m.state)
}

// CheckClose reports whether closing this mount from ctx would wait on the
// callback currently using its own lease.
func (m *Mount) CheckClose(ctx context.Context) error {
	if m == nil || m.state == nil {
		return nil
	}
	if state, _ := ctx.Value(callbackMountKey{}).(*mountState); state == m.state {
		return ErrSelfClose
	}
	return nil
}

func (m *Mount) Close(ctx context.Context) error {
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
	state     *mountState
}

func (r *Registry) Snapshot(target Scope) (*Plan, error) {
	return r.snapshot(target, nil)
}

// SnapshotInstances freezes only the named active mount instances. It is used
// for descriptor-driven resume so unrelated mounts added later are ignored.
func (r *Registry) SnapshotInstances(target Scope, instanceIDs []string) (*Plan, error) {
	allowed := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		allowed[id] = true
	}
	return r.snapshot(target, allowed)
}

func (r *Registry) snapshot(target Scope, allowed map[string]bool) (*Plan, error) {
	if r == nil {
		return &Plan{}, nil
	}
	if err := validateTargetScope(target); err != nil {
		return nil, err
	}
	r.mu.Lock()
	entries := make([]plannedEntry, 0)
	leased := make(map[*mountState]struct{})
	for _, state := range r.mounts {
		if !state.active {
			continue
		}
		if allowed != nil && !allowed[state.component.InstanceID] {
			continue
		}
		for _, entry := range state.entries {
			if !scopeApplies(entry.spec.Scope, target) {
				continue
			}
			entries = append(entries, plannedEntry{registrationEntry: entry, component: state.component, state: state})
			leased[state] = struct{}{}
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
	return &Plan{entries: entries, reporter: r.reporter, releases: releases}, nil
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
		result = append(result, PlanEntryDiagnostic{InstanceID: entry.spec.InstanceID, Artifact: entry.component.Artifact, ID: entry.spec.ID, Contract: entry.contract, Order: entry.spec.Order, Scope: entry.spec.Scope, Kind: kind})
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
	if left.spec.InstanceID != right.spec.InstanceID {
		return left.spec.InstanceID < right.spec.InstanceID
	}
	return left.spec.ID < right.spec.ID
}

func (r *Registry) release(state *mountState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state.refs--
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

type RegistrationDiagnostic struct {
	ID       string
	Contract Contract
	Order    int
	Scope    Scope
	Kind     string
}

type ComponentDiagnostic struct {
	InstanceID    string
	Artifact      Artifact
	Registrations []RegistrationDiagnostic
}

func (r *Registry) Diagnostics() []ComponentDiagnostic {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]ComponentDiagnostic, 0, len(r.mounts))
	for _, state := range r.mounts {
		item := ComponentDiagnostic{InstanceID: state.component.InstanceID, Artifact: state.component.Artifact}
		for _, entry := range state.entries {
			kind := "notification"
			if entry.kind == entryInterceptor {
				kind = "interceptor"
			}
			item.Registrations = append(item.Registrations, RegistrationDiagnostic{ID: entry.spec.ID, Contract: entry.contract, Order: entry.spec.Order, Scope: entry.spec.Scope, Kind: kind})
		}
		sort.Slice(item.Registrations, func(i, j int) bool {
			left, right := item.Registrations[i], item.Registrations[j]
			if left.Order != right.Order {
				return left.Order < right.Order
			}
			return left.ID < right.ID
		})
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].InstanceID < result[j].InstanceID })
	return result
}
