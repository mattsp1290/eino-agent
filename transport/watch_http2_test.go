package transport

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mattsp1290/eino-agent/session"
	"github.com/mattsp1290/eino-agent/watch"
)

type slowInitialWatchReader struct {
	session.ObservationReader
	once  sync.Once
	delay time.Duration
}

func (r *slowInitialWatchReader) ReadObservationSnapshot(ctx context.Context, id session.ID, limits session.ObservationLimits) (session.ObservationSnapshot, error) {
	r.once.Do(func() {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
		}
	})
	return r.ObservationReader.ReadObservationSnapshot(ctx, id, limits)
}

func TestSessionWatchHTTP2SurvivesIdleAndSlowInitialRead(t *testing.T) {
	for _, slowInitial := range []bool{false, true} {
		name := "idle"
		if slowInitial {
			name = "slow-initial-read"
		}
		t.Run(name, func(t *testing.T) {
			service, store, config := watchFixture(t)
			config.WriteTimeout = 50 * time.Millisecond
			if slowInitial {
				reader := &slowInitialWatchReader{ObservationReader: store, delay: 3 * config.WriteTimeout}
				var err error
				service, err = watch.NewService(reader, watch.Options{
					Snapshot:     session.ObservationLimits{MaxMessages: 10, MaxTools: 10, MaxParts: 20, MaxSnapshotBytes: 1 << 20, MaxTextBytes: 1 << 16},
					PollInterval: 5 * time.Millisecond, ReadTimeout: time.Second,
					MaxSubscriptions: 10, MaxWatchedSessions: 10, MaxLiveRuns: 10, MaxLiveTextBytes: 1 << 16, PendingUpdates: 10,
				})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = service.Close(context.Background()) }()
				config.Service = service
			}
			server := httptest.NewUnstartedServer(SessionWatchHandler(config))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
				t.Fatalf("unexpected response: %s %s", resp.Proto, resp.Status)
			}
			scanner := bufio.NewScanner(resp.Body)
			readState := func(want string) {
				t.Helper()
				for scanner.Scan() {
					line := scanner.Text()
					if strings.Contains(line, "STATE_SNAPSHOT") && strings.Contains(line, want) {
						return
					}
				}
				t.Fatalf("stream ended before %s: %v", want, scanner.Err())
			}
			readState(`"Exists":false`)
			// Let the HTTP/2 stream remain idle beyond the network write budget.
			time.Sleep(3 * config.WriteTimeout)
			if _, err := store.CreateSession(ctx, session.Session{ID: "s"}); err != nil {
				t.Fatal(err)
			}
			service.Hint("s")
			readState(`"Exists":true`)
		})
	}
}
