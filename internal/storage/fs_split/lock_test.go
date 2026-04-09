//go:build darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || windows

package fs_split

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriterOwnershipRejectsLiveOwner(t *testing.T) {
	t.Parallel()

	path := filepath.Join(testTempDir(t), lockName)
	first, err := acquireWriterLockWithConfig(path, lockConfig{now: fixedTime("2026-03-28T10:00:00Z")})
	if err != nil {
		t.Fatalf("acquire first writer lock: %v", err)
	}
	t.Cleanup(func() {
		if err := first.release(); err != nil {
			t.Errorf("release first writer lock: %v", err)
		}
	})

	if _, err := acquireWriterLockWithConfig(path, lockConfig{now: fixedTime("2026-03-28T10:01:00Z")}); !errors.Is(err, ErrWriterLocked) {
		t.Fatalf("acquire second writer lock error = %v, want ErrWriterLocked", err)
	}
}

func TestWriterOwnershipRecoversStaleMetadata(t *testing.T) {
	t.Parallel()

	path := filepath.Join(testTempDir(t), lockName)
	if err := ensureLockDirectory(path); err != nil {
		t.Fatal(err)
	}
	stale := ownerMetadata{
		PID:         1234,
		SessionID:   "00112233445566778899aabbccddeeff",
		StartedAt:   mustTime(t, "2026-03-28T09:00:00Z"),
		HeartbeatAt: mustTime(t, "2026-03-28T09:01:00Z"),
	}
	writeOwnerFixture(t, path, stale)

	lock, err := acquireWriterLockWithConfig(path, lockConfig{now: fixedTime("2026-03-28T10:00:00Z")})
	if err != nil {
		t.Fatalf("recover stale writer ownership: %v", err)
	}
	defer func() {
		if err := lock.release(); err != nil {
			t.Errorf("release recovered writer lock: %v", err)
		}
	}()
	if lock.owner.SessionID == stale.SessionID {
		t.Fatal("stale writer session id was reused")
	}
	owner, found, err := readOwnerMetadata(filepath.Join(path, lockOwnerName))
	if err != nil {
		t.Fatal(err)
	}
	if !found || owner.SessionID != lock.owner.SessionID || owner.PID != os.Getpid() {
		t.Fatalf("recovered owner metadata = %+v, want current owner %+v", owner, lock.owner)
	}
}

func TestWriterOwnershipFailsClosedForFreshOrMalformedMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content []byte
		want    error
	}{
		{
			name: "fresh",
			content: []byte(
				`{"pid":1234,"session_id":"00112233445566778899aabbccddeeff",` +
					`"started_at":"2026-03-28T09:59:00Z","heartbeat_at":"2026-03-28T09:59:50Z"}` + "\n",
			),
			want: ErrWriterLocked,
		},
		{
			name:    "malformed",
			content: []byte("{not-json\n"),
			want:    ErrOwnerMetadata,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(testTempDir(t), lockName)
			if err := ensureLockDirectory(path); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(path, lockOwnerName), test.content, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := acquireWriterLockWithConfig(path, lockConfig{now: fixedTime("2026-03-28T10:00:00Z")}); !errors.Is(err, test.want) {
				t.Fatalf("acquire writer lock error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestWriterOwnershipHeartbeatAndReleaseLifecycle(t *testing.T) {
	t.Parallel()

	path := filepath.Join(testTempDir(t), lockName)
	heartbeat := make(chan time.Time)
	heartbeatDone := make(chan struct{}, 1)
	started := mustTime(t, "2026-03-28T10:00:00Z")
	lock, err := acquireWriterLockWithConfig(path, lockConfig{
		now:       func() time.Time { return started },
		heartbeat: heartbeat,
		afterHeartbeat: func() {
			heartbeatDone <- struct{}{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	next := started.Add(writerHeartbeatInterval)
	heartbeat <- next
	<-heartbeatDone
	owner, found, err := readOwnerMetadata(filepath.Join(path, lockOwnerName))
	if err != nil {
		t.Fatal(err)
	}
	if !found || !owner.StartedAt.Equal(started) || !owner.HeartbeatAt.Equal(next) {
		t.Fatalf("owner metadata after heartbeat = %+v", owner)
	}
	if err := lock.release(); err != nil {
		t.Fatalf("release writer lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(path, lockOwnerName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owner metadata remains after release: %v", err)
	}

	reopened, err := acquireWriterLockAfterRelease(path, lockConfig{now: func() time.Time { return next }})
	if err != nil {
		t.Fatalf("acquire writer lock after release: %v", err)
	}
	if err := reopened.release(); err != nil {
		t.Fatalf("release reopened writer lock: %v", err)
	}
}

func fixedTime(value string) func() time.Time {
	return func() time.Time {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			panic(err)
		}
		return parsed
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func writeOwnerFixture(t *testing.T, directory string, owner ownerMetadata) {
	t.Helper()
	lock := writerLock{directory: directory, owner: owner}
	if err := lock.writeOwner(); err != nil {
		t.Fatal(err)
	}
}
