package server

import (
	"context"
	"net"
	"sync"
	"time"
)

type authSourceState struct {
	failures    int
	bannedUntil time.Time
}

type authFailureTracker struct {
	mu          sync.Mutex
	sources     map[string]authSourceState
	delay       time.Duration
	limit       int
	banDuration time.Duration
}

func newAuthFailureTracker(delay time.Duration, limit int, banDuration time.Duration) *authFailureTracker {
	return &authFailureTracker{
		sources:     make(map[string]authSourceState),
		delay:       delay,
		limit:       limit,
		banDuration: banDuration,
	}
}

func (t *authFailureTracker) registerFailure(source string, now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.sources[source]
	if now.Before(state.bannedUntil) {
		return t.delay
	}
	state.failures++
	if state.failures >= t.limit {
		state.failures = 0
		state.bannedUntil = now.Add(t.banDuration)
	}
	t.sources[source] = state
	return t.delay
}

func (t *authFailureTracker) banned(source string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.sources[source]
	if !ok {
		return false
	}
	if now.Before(state.bannedUntil) {
		return true
	}
	state.bannedUntil = time.Time{}
	t.sources[source] = state
	return false
}

func (t *authFailureTracker) authenticated(source string) {
	t.mu.Lock()
	delete(t.sources, source)
	t.mu.Unlock()
}

func authSource(connection net.Conn) string {
	host, _, err := net.SplitHostPort(connection.RemoteAddr().String())
	if err == nil {
		return host
	}
	return connection.RemoteAddr().String()
}

func waitForAuthPenalty(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
