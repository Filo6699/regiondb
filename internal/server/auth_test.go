package server

import (
	"testing"
	"time"
)

func TestAuthFailureTrackerDelaysAndTemporarilyBansSource(t *testing.T) {
	t.Parallel()

	tracker := newAuthFailureTracker(5*time.Millisecond, 2, time.Minute)
	now := time.Unix(100, 0)
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("first failure delay = %v", got)
	}
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("second failure delay = %v", got)
	}
	if !tracker.banned("192.0.2.1", now.Add(10*time.Second)) {
		t.Fatal("source was not temporarily banned")
	}
	if got := tracker.registerFailure("192.0.2.2", now); got != 5*time.Millisecond {
		t.Fatalf("independent source delay = %v", got)
	}

	tracker.authenticated("192.0.2.1")
	if tracker.banned("192.0.2.1", now) {
		t.Fatal("source remained banned after successful authentication")
	}
	if got := tracker.registerFailure("192.0.2.1", now); got != 5*time.Millisecond {
		t.Fatalf("delay after successful authentication = %v", got)
	}
}
