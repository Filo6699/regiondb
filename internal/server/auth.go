package server

import (
	"container/list"
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/Filo6699/regiondb/internal/telemetry"
)

const maxAuthSources = 4096

type authSourceState struct {
	failures    int
	bannedUntil time.Time
	recency     *list.Element
}

type authFailureTracker struct {
	mu          sync.Mutex
	sources     map[string]authSourceState
	recency     list.List
	delay       time.Duration
	limit       int
	banDuration time.Duration
	metrics     *telemetry.Metrics
}

func newAuthFailureTracker(
	delay time.Duration,
	limit int,
	banDuration time.Duration,
	metrics ...*telemetry.Metrics,
) *authFailureTracker {
	var registry *telemetry.Metrics
	if len(metrics) != 0 {
		registry = metrics[0]
	}
	return &authFailureTracker{
		sources:     make(map[string]authSourceState),
		delay:       delay,
		limit:       limit,
		banDuration: banDuration,
		metrics:     registry,
	}
}

func (t *authFailureTracker) registerFailure(source string, now time.Time) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.metrics != nil {
		t.metrics.AuthFailure()
	}
	state, exists := t.sources[source]
	if now.Before(state.bannedUntil) {
		t.touch(state.recency)
		return t.delay
	}
	if !exists && !t.makeRoom(now) {
		return t.delay
	}
	if !exists {
		state.recency = t.recency.PushFront(source)
	}
	state.failures++
	if state.failures >= t.limit {
		state.failures = 0
		state.bannedUntil = now.Add(t.banDuration)
		if t.metrics != nil {
			t.metrics.AuthBan()
		}
	}
	t.touch(state.recency)
	t.sources[source] = state
	t.updateSourceGauge()
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
		t.touch(state.recency)
		t.sources[source] = state
		return true
	}
	state.bannedUntil = time.Time{}
	t.touch(state.recency)
	t.sources[source] = state
	return false
}

func (t *authFailureTracker) authenticated(source string) {
	t.mu.Lock()
	if state, ok := t.sources[source]; ok {
		t.recency.Remove(state.recency)
		delete(t.sources, source)
		t.updateSourceGauge()
	}
	t.mu.Unlock()
}

func authSource(connection net.Conn) string {
	host, _, err := net.SplitHostPort(connection.RemoteAddr().String())
	if err != nil {
		host = connection.RemoteAddr().String()
	}
	address, parseErr := netip.ParseAddr(host)
	if parseErr != nil {
		return host
	}
	address = address.Unmap()
	if address.Is6() {
		address = address.WithZone("")
		return netip.PrefixFrom(address, 64).Masked().String()
	}
	return address.String()
}

func (t *authFailureTracker) makeRoom(now time.Time) bool {
	if len(t.sources) < maxAuthSources {
		return true
	}
	for element := t.recency.Back(); element != nil; element = element.Prev() {
		source := element.Value.(string)
		state := t.sources[source]
		if now.Before(state.bannedUntil) {
			continue
		}
		delete(t.sources, source)
		t.recency.Remove(element)
		return true
	}
	return false
}

func (t *authFailureTracker) touch(element *list.Element) {
	if element != nil {
		t.recency.MoveToFront(element)
	}
}

func (t *authFailureTracker) updateSourceGauge() {
	if t.metrics != nil {
		t.metrics.SetAuthSources(len(t.sources))
	}
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
