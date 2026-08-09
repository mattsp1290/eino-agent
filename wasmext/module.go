package wasmext

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type module struct {
	identity  moduleIdentity
	limits    Limits
	engine    engine
	component compiledComponent
	mu        sync.Mutex
	closed    bool
	inFlight  sync.WaitGroup
	closeOnce sync.Once
}

func loadModule(ctx context.Context, cfg ModuleConfig, contract worldContract, factory engineFactory) (*module, error) {
	limits := cfg.Limits.withDefaults()
	identity := moduleIdentity{name: strings.TrimSpace(cfg.Name), hash: strings.ToLower(strings.TrimSpace(cfg.ExpectedSHA256))}
	if identity.name == "" || len(identity.name) > 128 {
		return nil, extensionError(ErrorConfig, identity, "load", errors.New("invalid module name"))
	}
	var configBytes int64
	for key, value := range cfg.GuestConfig {
		configBytes += int64(len(key) + len(value))
	}
	if configBytes > limits.MaxInputBytes {
		return nil, extensionError(ErrorSize, identity, "load", errors.New("guest configuration exceeds bound"))
	}
	expected, err := hex.DecodeString(identity.hash)
	if err != nil || len(expected) != sha256.Size {
		return nil, extensionError(ErrorConfig, identity, "load", errors.New("invalid sha256"))
	}
	path, err := secureModulePath(cfg.AllowedRoot, cfg.Path)
	if err != nil {
		return nil, extensionError(ErrorPath, identity, "load", err)
	}
	bytes, err := readBoundedFile(path, limits.MaxModuleBytes)
	if err != nil {
		kind := ErrorEngine
		if errors.Is(err, errModuleTooLarge) {
			kind = ErrorSize
		}
		return nil, extensionError(kind, identity, "load", err)
	}
	actual := sha256.Sum256(bytes)
	if subtle.ConstantTimeCompare(actual[:], expected) != 1 {
		return nil, extensionError(ErrorHash, identity, "load", errors.New("hash mismatch"))
	}
	engine, err := factory(limits)
	if err != nil {
		return nil, extensionError(ErrorEngine, identity, "compile", err)
	}
	component, err := engine.Compile(ctx, bytes, contract)
	if err != nil {
		_ = engine.Close()
		return nil, extensionError(ErrorContract, identity, "compile", err)
	}
	return &module{identity: identity, limits: limits, engine: engine, component: component}, nil
}

var errModuleTooLarge = errors.New("module exceeds size bound")

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("module is not a regular file")
	}
	if info.Size() > limit {
		return nil, errModuleTooLarge
	}
	bytes, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(bytes)) > limit {
		return nil, errModuleTooLarge
	}
	return bytes, nil
}

func secureModulePath(root, path string) (string, error) {
	if root == "" || path == "" {
		return "", errors.New("allowed root and path required")
	}
	if parsed, err := url.Parse(path); err == nil && parsed.Scheme != "" {
		return "", errors.New("URL modules are not allowed")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	rootReal, err := filepath.EvalSymlinks(rootAbs)
	if err != nil {
		return "", err
	}
	candidate := path
	if !filepath.IsAbs(candidate) {
		candidate = filepath.Join(rootReal, candidate)
	}
	candidateReal, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootReal, candidateReal)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", errors.New("module path escapes allowed root")
	}
	return candidateReal, nil
}

func (m *module) call(ctx context.Context, operation string, inputBytes int, input, output any) error {
	if int64(inputBytes) > m.limits.MaxInputBytes {
		return extensionError(ErrorSize, m.identity, operation, errors.New("input exceeds bound"))
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return extensionError(ErrorClosed, m.identity, operation, nil)
	}
	m.inFlight.Add(1)
	m.mu.Unlock()
	defer m.inFlight.Done()
	callCtx, cancel := context.WithTimeout(ctx, m.limits.Timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- m.component.Call(callCtx, operation, input, output) }()
	select {
	case err := <-done:
		if err != nil {
			return extensionError(ErrorTrap, m.identity, operation, err)
		}
		return nil
	case <-callCtx.Done():
		m.component.Interrupt()
		select {
		case <-done:
		case <-time.After(m.limits.CloseDrain):
		}
		return extensionError(ErrorTimeout, m.identity, operation, callCtx.Err())
	}
}

func (m *module) Close() error {
	var closeErr error
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		m.component.Interrupt()
		done := make(chan struct{})
		go func() { m.inFlight.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(m.limits.CloseDrain):
			closeErr = extensionError(ErrorTimeout, m.identity, "close", context.DeadlineExceeded)
		}
		closeErr = errors.Join(closeErr, m.component.Close(), m.engine.Close())
	})
	return closeErr
}

func validateBoundedJSON(raw []byte, limit int64) error {
	if int64(len(raw)) > limit {
		return errModuleTooLarge
	}
	if !jsonValid(raw) {
		return fmt.Errorf("malformed JSON")
	}
	return nil
}

var jsonValid = func(raw []byte) bool {
	// Kept as a small variable to make malformed-payload tests independent of
	// the component engine.
	return len(raw) > 0 && json.Valid(raw)
}
