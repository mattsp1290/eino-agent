package wasmext

import (
	"errors"
	"fmt"
	"time"

	einoobs "github.com/mattsp1290/eino-obs"
)

const (
	defaultModuleBytes = int64(16 << 20)
	defaultMemoryBytes = int64(64 << 20)
	defaultInputBytes  = int64(256 << 10)
	defaultOutputBytes = int64(256 << 10)
	defaultTimeout     = 2 * time.Second
	defaultCloseDrain  = 2 * time.Second
)

// ErrorKind is a stable failure class for Wasm extension operations.
type ErrorKind string

const (
	ErrorConfig   ErrorKind = "config"
	ErrorPath     ErrorKind = "path"
	ErrorHash     ErrorKind = "hash"
	ErrorSize     ErrorKind = "size"
	ErrorContract ErrorKind = "contract"
	ErrorTrap     ErrorKind = "trap"
	ErrorTimeout  ErrorKind = "timeout"
	ErrorClosed   ErrorKind = "closed"
	ErrorPayload  ErrorKind = "payload"
	ErrorEngine   ErrorKind = "engine"
)

// Error is a bounded, classified extension failure. Error strings identify a
// module only by host-configured name and verified hash; guest-provided strings
// and filesystem paths are not included.
type Error struct {
	Kind       ErrorKind
	ModuleName string
	ModuleHash string
	Operation  string
	cause      error
}

func (e *Error) Error() string {
	if e == nil {
		return "wasm extension error"
	}
	return fmt.Sprintf("wasm extension %s failed for module %q (%s): %s", e.Operation, e.ModuleName, shortHash(e.ModuleHash), e.Kind)
}

// Is allows callers to match an ErrorKind using a target *Error.
func (e *Error) Is(target error) bool {
	want, ok := target.(*Error)
	return ok && (want.Kind == "" || e.Kind == want.Kind)
}

func extensionError(kind ErrorKind, identity moduleIdentity, operation string, cause error) error {
	return &Error{Kind: kind, ModuleName: identity.name, ModuleHash: identity.hash, Operation: operation, cause: cause}
}

func shortHash(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

// IsKind reports whether err is a classified Wasm extension error of kind.
func IsKind(err error, kind ErrorKind) bool {
	return errors.Is(err, &Error{Kind: kind})
}

// Limits are nonzero host-enforced resource bounds. Zero values select safe
// defaults; supplied values may tighten them.
type Limits struct {
	MaxModuleBytes int64
	MaxMemoryBytes int64
	MaxInputBytes  int64
	MaxOutputBytes int64
	Timeout        time.Duration
	CloseDrain     time.Duration
}

func (l Limits) withDefaults() Limits {
	if l.MaxModuleBytes <= 0 {
		l.MaxModuleBytes = defaultModuleBytes
	}
	if l.MaxMemoryBytes <= 0 {
		l.MaxMemoryBytes = defaultMemoryBytes
	}
	if l.MaxInputBytes <= 0 {
		l.MaxInputBytes = defaultInputBytes
	}
	if l.MaxOutputBytes <= 0 {
		l.MaxOutputBytes = defaultOutputBytes
	}
	if l.Timeout <= 0 {
		l.Timeout = defaultTimeout
	}
	if l.CloseDrain <= 0 {
		l.CloseDrain = defaultCloseDrain
	}
	return l
}

// ModuleConfig identifies one explicitly configured local component.
type ModuleConfig struct {
	Name           string
	Path           string
	AllowedRoot    string
	ExpectedSHA256 string
	Limits         Limits
	// Observer receives bounded guest log observations with the configured
	// module name and verified digest attached. A nil observer drops guest logs.
	Observer *einoobs.Observer
	// GuestConfig is reserved for bounded, non-secret extension configuration.
	// v0.1 worlds currently expose no configuration import, so these values are
	// validated and retained by the host but never sent to a guest.
	GuestConfig map[string]string
}

type moduleIdentity struct{ name, hash string }
