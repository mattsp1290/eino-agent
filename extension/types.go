package extension

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidContract         = errors.New("invalid extension contract")
	ErrInvalidComponent        = errors.New("invalid extension component")
	ErrInvalidRegistration     = errors.New("invalid extension registration")
	ErrDuplicateRegistration   = errors.New("duplicate extension registration")
	ErrDuplicateInstance       = errors.New("duplicate extension instance")
	ErrProceedCalledTwice      = errors.New("extension proceed called more than once")
	ErrProceedNotCalled        = errors.New("extension proceed was not called")
	ErrProceedOutlivedCallback = errors.New("extension proceed outlived its callback")
	ErrProtectedMutation       = errors.New("extension changed protected input")
	ErrMountClosed             = errors.New("extension mount closed")
	ErrSelfClose               = errors.New("extension callback cannot wait for its own mount")
)

type SourceKind string

const (
	SourceNative SourceKind = "native"
	SourceWasm   SourceKind = "wasm"
)

type Artifact struct {
	Name       string
	Version    string
	Hash       string
	ConfigHash string
	SourceKind SourceKind
}

type Component struct {
	InstanceID string
	Artifact   Artifact
}

type ScopeKind string

const (
	ScopeGlobal  ScopeKind = "global"
	ScopeSession ScopeKind = "session"
)

type Scope struct {
	Kind ScopeKind
	Key  string
}

func GlobalScope() Scope           { return Scope{Kind: ScopeGlobal} }
func SessionScope(id string) Scope { return Scope{Kind: ScopeSession, Key: id} }

type Registration struct {
	ID    string
	Order int
	Scope Scope
}

type Contract struct {
	ID      string
	Version string
}

type CloneFunc[T any] func(T) (T, error)
type ValidateFunc[T any] func(T) error
type NextValidator[I any] func(original, candidate I) error
type ContinueFunc[D any] func(D) bool

// Proceed synchronously delegates to the remainder of an around-callback chain.
// It may be called at most once, must finish before the Around callback returns,
// and does not support concurrent use. Terminal output remains host-owned.
type Proceed func(context.Context) error

// Around wraps a point and may synchronously call proceed at most once. A
// callback receives an isolated input clone and cannot replace terminal output.
type Around[I any] func(context.Context, I, Proceed) error
type HookFunc[T any] func(context.Context, T) error
type TransformFunc[T any] func(context.Context, T) (T, error)
type GateFunc[I, D any] func(context.Context, I) (D, error)
type Observer[T any] func(context.Context, T) error
type Cleanup func(context.Context) error

type HandlerKind string

const (
	HandlerNotification HandlerKind = "notification"
	HandlerHook         HandlerKind = "hook"
	HandlerTransform    HandlerKind = "transform"
	HandlerGate         HandlerKind = "gate"
	HandlerAround       HandlerKind = "around"
)

type durablePointKey struct {
	contract Contract
	kind     HandlerKind
}

// Point is a host-declared extension point. The unexported method keeps point
// authority limited to the typed handles defined by this package.
type Point interface {
	extensionPoint() *pointDefinition
}

// pointDefinition is the immutable, process-local authority for one extension
// point. Typed definitions own both this identity and the policies used when
// dispatching it, so copying a point cannot separate identity from semantics.
type pointDefinition struct {
	durablePointKey
}

type Notification[T any] struct {
	definition *notificationDefinition[T]
}

type notificationDefinition[T any] struct {
	point pointDefinition
	clone CloneFunc[T]
}

func NewNotification[T any](contract Contract, clone CloneFunc[T]) Notification[T] {
	return Notification[T]{definition: &notificationDefinition[T]{point: newPointDefinition(contract, HandlerNotification), clone: clone}}
}

func (p Notification[T]) Contract() Contract { return pointContract(p.base()) }
func (p Notification[T]) base() *pointDefinition {
	if p.definition == nil {
		return nil
	}
	return &p.definition.point
}
func (p Notification[T]) extensionPoint() *pointDefinition { return p.base() }

type Hook[T any] struct {
	definition *hookDefinition[T]
}

type hookDefinition[T any] struct {
	point    pointDefinition
	clone    CloneFunc[T]
	validate NextValidator[T]
}

func NewHook[T any](contract Contract, clone CloneFunc[T], validate NextValidator[T]) Hook[T] {
	return Hook[T]{definition: &hookDefinition[T]{point: newPointDefinition(contract, HandlerHook), clone: clone, validate: validate}}
}

func (p Hook[T]) Contract() Contract { return pointContract(p.base()) }
func (p Hook[T]) base() *pointDefinition {
	if p.definition == nil {
		return nil
	}
	return &p.definition.point
}
func (p Hook[T]) extensionPoint() *pointDefinition { return p.base() }

type Transform[T any] struct {
	definition *transformDefinition[T]
}

type transformDefinition[T any] struct {
	point    pointDefinition
	clone    CloneFunc[T]
	validate NextValidator[T]
}

func NewTransform[T any](contract Contract, clone CloneFunc[T], validate NextValidator[T]) Transform[T] {
	return Transform[T]{definition: &transformDefinition[T]{point: newPointDefinition(contract, HandlerTransform), clone: clone, validate: validate}}
}

func (p Transform[T]) Contract() Contract { return pointContract(p.base()) }
func (p Transform[T]) base() *pointDefinition {
	if p.definition == nil {
		return nil
	}
	return &p.definition.point
}
func (p Transform[T]) extensionPoint() *pointDefinition { return p.base() }

type Gate[I, D any] struct {
	definition *gateDefinition[I, D]
}

type gateDefinition[I, D any] struct {
	point            pointDefinition
	clone            CloneFunc[I]
	validateInput    NextValidator[I]
	validateDecision ValidateFunc[D]
	continueDecision D
	shouldContinue   ContinueFunc[D]
}

func NewGate[I, D any](contract Contract, clone CloneFunc[I], validateInput NextValidator[I], validateDecision ValidateFunc[D], continueDecision D, shouldContinue ContinueFunc[D]) Gate[I, D] {
	return Gate[I, D]{definition: &gateDefinition[I, D]{point: newPointDefinition(contract, HandlerGate), clone: clone, validateInput: validateInput, validateDecision: validateDecision, continueDecision: continueDecision, shouldContinue: shouldContinue}}
}

func (p Gate[I, D]) Contract() Contract { return pointContract(p.base()) }
func (p Gate[I, D]) base() *pointDefinition {
	if p.definition == nil {
		return nil
	}
	return &p.definition.point
}
func (p Gate[I, D]) extensionPoint() *pointDefinition { return p.base() }

type RequiredAround[I, O any] struct {
	definition *aroundDefinition[I, O]
}

type aroundDefinition[I, O any] struct {
	point          pointDefinition
	clone          CloneFunc[I]
	validateOutput ValidateFunc[O]
}

func NewRequiredAround[I, O any](contract Contract, clone CloneFunc[I], validateOutput ValidateFunc[O]) RequiredAround[I, O] {
	return RequiredAround[I, O]{definition: &aroundDefinition[I, O]{point: newPointDefinition(contract, HandlerAround), clone: clone, validateOutput: validateOutput}}
}

func (p RequiredAround[I, O]) Contract() Contract { return pointContract(p.base()) }
func (p RequiredAround[I, O]) base() *pointDefinition {
	if p.definition == nil {
		return nil
	}
	return &p.definition.point
}
func (p RequiredAround[I, O]) extensionPoint() *pointDefinition { return p.base() }

func newPointDefinition(contract Contract, kind HandlerKind) pointDefinition {
	return pointDefinition{durablePointKey: durablePointKey{contract: contract, kind: kind}}
}

func pointContract(definition *pointDefinition) Contract {
	if definition == nil {
		return Contract{}
	}
	return definition.contract
}

// CallbackError is the bounded public error returned when a callback itself
// fails. The raw cause remains available through errors.Is/errors.As
// for trusted in-process diagnostics, while Error deliberately contains only
// host-owned point and registration identities.
type CallbackError struct {
	Point      Contract
	InstanceID string
	HandlerID  string
	Code       string
	cause      error
}

func (e *CallbackError) Error() string {
	if e == nil {
		return "extension callback failed"
	}
	return fmt.Sprintf("extension callback failed: point=%s instance=%s handler=%s code=%s", e.Point.ID, e.InstanceID, e.HandlerID, e.Code)
}

// Unwrap exposes the local raw cause without incorporating it into public
// error text.
func (e *CallbackError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

type Diagnostic struct {
	Point      Contract
	InstanceID string
	HandlerID  string
	Code       string
	Cause      error
}

type Reporter interface {
	Report(context.Context, Diagnostic)
}
type ReporterFunc func(context.Context, Diagnostic)

func (f ReporterFunc) Report(ctx context.Context, diagnostic Diagnostic) { f(ctx, diagnostic) }

type Installer interface {
	Install(context.Context, Registrar) error
}
type InstallerFunc func(context.Context, Registrar) error

func (f InstallerFunc) Install(ctx context.Context, registrar Registrar) error {
	return f(ctx, registrar)
}

type Registrar interface {
	register(registrationEntry) error
	InstanceID() string
	Defer(Cleanup) error
}

type registrationEntry struct {
	point    *pointDefinition
	spec     Registration
	callback any
}

func On[T any](registrar Registrar, point Notification[T], spec Registration, fn Observer[T]) error {
	if registrar == nil || fn == nil {
		return fmt.Errorf("%w: nil registrar or observer", ErrInvalidRegistration)
	}
	if err := validatePoint(point.base()); err != nil {
		return err
	}
	return registrar.register(registrationEntry{point: point.base(), spec: spec, callback: fn})
}

func OnHook[T any](registrar Registrar, point Hook[T], spec Registration, fn HookFunc[T]) error {
	if fn == nil {
		return fmt.Errorf("%w: nil callback", ErrInvalidRegistration)
	}
	return registerCallback(registrar, point.base(), spec, fn)
}

func OnTransform[T any](registrar Registrar, point Transform[T], spec Registration, fn TransformFunc[T]) error {
	if fn == nil {
		return fmt.Errorf("%w: nil callback", ErrInvalidRegistration)
	}
	return registerCallback(registrar, point.base(), spec, fn)
}

func OnGate[I, D any](registrar Registrar, point Gate[I, D], spec Registration, fn GateFunc[I, D]) error {
	if fn == nil {
		return fmt.Errorf("%w: nil callback", ErrInvalidRegistration)
	}
	return registerCallback(registrar, point.base(), spec, fn)
}

func OnAround[I, O any](registrar Registrar, point RequiredAround[I, O], spec Registration, fn Around[I]) error {
	if fn == nil {
		return fmt.Errorf("%w: nil callback", ErrInvalidRegistration)
	}
	return registerCallback(registrar, point.base(), spec, fn)
}

func registerCallback(registrar Registrar, point *pointDefinition, spec Registration, callback any) error {
	if registrar == nil || callback == nil {
		return fmt.Errorf("%w: nil registrar or callback", ErrInvalidRegistration)
	}
	if err := validatePoint(point); err != nil {
		return err
	}
	return registrar.register(registrationEntry{point: point, spec: spec, callback: callback})
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

// ValidateIdentifier verifies one stable extension-owned identifier.
func ValidateIdentifier(id string) error {
	if !identifierPattern.MatchString(id) {
		return fmt.Errorf("%w: stable identifier required", ErrInvalidRegistration)
	}
	return nil
}

// ValidateContract verifies the stable identity of an extension point.
func ValidateContract(contract Contract) error {
	if err := ValidateIdentifier(contract.ID); err != nil || strings.TrimSpace(contract.Version) == "" || len(contract.Version) > 64 {
		return fmt.Errorf("%w: stable id and version required", ErrInvalidContract)
	}
	return nil
}

func validatePoint(point *pointDefinition) error {
	if point == nil || point.kind == "" {
		return fmt.Errorf("%w: stable id and version required", ErrInvalidContract)
	}
	return ValidateContract(point.contract)
}

// ValidateScope verifies one global or session extension scope. Session keys
// are opaque and are deliberately not parsed as identifiers.
func ValidateScope(scope Scope) error {
	switch scope.Kind {
	case ScopeGlobal:
		if scope.Key != "" {
			return fmt.Errorf("%w: global scope key must be empty", ErrInvalidRegistration)
		}
	case ScopeSession:
		if scope.Key == "" {
			return fmt.Errorf("%w: session key required", ErrInvalidRegistration)
		}
	default:
		return fmt.Errorf("%w: unsupported scope", ErrInvalidRegistration)
	}
	return nil
}

// ValidateComponent verifies one extension instance and its artifact identity.
func ValidateComponent(component Component) error {
	artifact := component.Artifact
	if ValidateIdentifier(component.InstanceID) != nil || ValidateIdentifier(artifact.Name) != nil || strings.TrimSpace(artifact.Version) == "" || artifact.Hash == "" || artifact.ConfigHash == "" {
		return fmt.Errorf("%w: stable instance and artifact identity required", ErrInvalidComponent)
	}
	if artifact.SourceKind != SourceNative && artifact.SourceKind != SourceWasm {
		return fmt.Errorf("%w: unsupported source kind", ErrInvalidComponent)
	}
	return nil
}
