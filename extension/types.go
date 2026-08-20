package extension

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidContract       = errors.New("invalid extension contract")
	ErrInvalidComponent      = errors.New("invalid extension component")
	ErrInvalidRegistration   = errors.New("invalid extension registration")
	ErrDuplicateRegistration = errors.New("duplicate extension registration")
	ErrDuplicateInstance     = errors.New("duplicate extension instance")
	ErrNextCalledTwice       = errors.New("extension next called more than once")
	ErrNextNotCalled         = errors.New("extension next was not called")
	ErrProtectedMutation     = errors.New("extension changed protected input")
	ErrMountClosed           = errors.New("extension mount closed")
	ErrSelfClose             = errors.New("extension callback cannot wait for its own mount")
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
	ID         string
	InstanceID string
	Order      int
	Scope      Scope
}

type Contract struct {
	ID      string
	Version string
}

type NotificationPolicy uint8

const (
	NotificationContained NotificationPolicy = iota
	NotificationReturnFailures
)

type CloneFunc[T any] func(T) T
type ValidateFunc[T any] func(T) error
type OutputValidator[I, O any] func(original I, output O) error
type DelegatedOutputValidator[O any] func(delegated, returned O) error
type NextValidator[I any] func(original, candidate I) error
type Next[I, O any] func(context.Context, I) (O, error)
type Around[I, O any] func(context.Context, I, Next[I, O]) (O, error)
type Observer[T any] func(context.Context, T) error
type Cleanup func(context.Context) error

// pointKey must be non-zero-sized: Go permits pointers to distinct zero-sized
// variables to compare equal, which can alias otherwise unrelated points.
type pointKey [1]byte

type Notification[T any] struct {
	key      *pointKey
	contract Contract
	policy   NotificationPolicy
	clone    CloneFunc[T]
}

func NewNotification[T any](contract Contract, policy NotificationPolicy, clone CloneFunc[T]) Notification[T] {
	return Notification[T]{key: &pointKey{}, contract: contract, policy: policy, clone: clone}
}

func (p Notification[T]) Contract() Contract { return p.contract }

type Interceptor[I, O any] struct {
	key               *pointKey
	contract          Contract
	clone             CloneFunc[I]
	validateIn        NextValidator[I]
	validateOut       ValidateFunc[O]
	validateResult    OutputValidator[I, O]
	validateDelegated DelegatedOutputValidator[O]
	requireNext       bool
}

// NewInterceptorWithResultValidation additionally validates each returned
// value against the original protected input.
func NewInterceptorWithResultValidation[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O], validateResult OutputValidator[I, O]) Interceptor[I, O] {
	point := NewInterceptor(contract, clone, validateNext, validateOut)
	point.validateResult = validateResult
	return point
}

// NewRequiredInterceptorWithResultValidation combines successful-delegation
// enforcement with protected return-value validation.
func NewRequiredInterceptorWithResultValidation[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O], validateResult OutputValidator[I, O]) Interceptor[I, O] {
	point := NewInterceptorWithResultValidation(contract, clone, validateNext, validateOut, validateResult)
	point.requireNext = true
	return point
}

// NewRequiredDelegatingInterceptor requires one successful delegation and
// lets the point verify that an interceptor returned the delegated value. It
// is intended for opaque outputs such as provider stream handles.
func NewRequiredDelegatingInterceptor[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O], validateDelegated DelegatedOutputValidator[O]) Interceptor[I, O] {
	point := NewRequiredInterceptor(contract, clone, validateNext, validateOut)
	point.validateDelegated = validateDelegated
	return point
}

func NewInterceptor[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O]) Interceptor[I, O] {
	return Interceptor[I, O]{key: &pointKey{}, contract: contract, clone: clone, validateIn: validateNext, validateOut: validateOut}
}

func NewRequiredInterceptor[I, O any](contract Contract, clone CloneFunc[I], validateNext NextValidator[I], validateOut ValidateFunc[O]) Interceptor[I, O] {
	point := NewInterceptor(contract, clone, validateNext, validateOut)
	point.requireNext = true
	return point
}

func (p Interceptor[I, O]) Contract() Contract { return p.contract }

type Failure struct {
	Point      Contract
	InstanceID string
	HandlerID  string
	Code       string
	Cause      error
}

// CallbackError is the bounded public error returned when an interceptor
// itself fails. The raw cause remains available through errors.Is/errors.As
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

type Failures []Failure

func (f Failures) Error() string {
	if len(f) == 0 {
		return ""
	}
	return fmt.Sprintf("%d extension callback failure(s)", len(f))
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
	Defer(Cleanup) error
}

type entryKind uint8

const (
	entryNotification entryKind = iota
	entryInterceptor
)

type registrationEntry struct {
	point       *pointKey
	contract    Contract
	spec        Registration
	kind        entryKind
	callback    any
	policy      NotificationPolicy
	requireNext bool
}

func On[T any](registrar Registrar, point Notification[T], spec Registration, fn Observer[T]) error {
	if registrar == nil || fn == nil {
		return fmt.Errorf("%w: nil registrar or observer", ErrInvalidRegistration)
	}
	if err := validatePoint(point.key, point.contract); err != nil {
		return err
	}
	return registrar.register(registrationEntry{point: point.key, contract: point.contract, spec: spec, kind: entryNotification, callback: fn, policy: point.policy})
}

func Use[I, O any](registrar Registrar, point Interceptor[I, O], spec Registration, fn Around[I, O]) error {
	if registrar == nil || fn == nil {
		return fmt.Errorf("%w: nil registrar or interceptor", ErrInvalidRegistration)
	}
	if err := validatePoint(point.key, point.contract); err != nil {
		return err
	}
	return registrar.register(registrationEntry{point: point.key, contract: point.contract, spec: spec, kind: entryInterceptor, callback: fn, requireNext: point.requireNext})
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$`)

func validatePoint(key *pointKey, contract Contract) error {
	if key == nil || !identifierPattern.MatchString(contract.ID) || strings.TrimSpace(contract.Version) == "" || len(contract.Version) > 64 {
		return fmt.Errorf("%w: stable id and version required", ErrInvalidContract)
	}
	return nil
}

func validateScope(scope Scope) error {
	switch scope.Kind {
	case ScopeGlobal:
		if scope.Key != "" {
			return fmt.Errorf("%w: global scope key must be empty", ErrInvalidRegistration)
		}
	case ScopeSession:
		if !identifierPattern.MatchString(scope.Key) {
			return fmt.Errorf("%w: session key required", ErrInvalidRegistration)
		}
	default:
		return fmt.Errorf("%w: unsupported scope", ErrInvalidRegistration)
	}
	return nil
}

func validateArtifact(component Component) error {
	artifact := component.Artifact
	if !identifierPattern.MatchString(component.InstanceID) || !identifierPattern.MatchString(artifact.Name) || strings.TrimSpace(artifact.Version) == "" || artifact.Hash == "" || artifact.ConfigHash == "" {
		return fmt.Errorf("%w: stable instance and artifact identity required", ErrInvalidComponent)
	}
	if artifact.SourceKind != SourceNative && artifact.SourceKind != SourceWasm {
		return fmt.Errorf("%w: unsupported source kind", ErrInvalidComponent)
	}
	return nil
}
