package fs_split

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	lockName                = ".regiondb.lock"
	lockGuardName           = "guard"
	lockOwnerName           = "owner.json"
	writerHeartbeatInterval = 5 * time.Second
	writerStaleAfter        = 30 * time.Second
)

var (
	ErrWriterLocked        = errors.New("data directory already has a writer")
	ErrWriterOwnershipLost = errors.New("writer ownership lost")
	ErrOwnerMetadata       = errors.New("invalid writer owner metadata")
)

type ownerMetadata struct {
	PID         int       `json:"pid"`
	SessionID   string    `json:"session_id"`
	StartedAt   time.Time `json:"started_at"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

type lockConfig struct {
	now            func() time.Time
	heartbeat      <-chan time.Time
	afterHeartbeat func()
}

type writerLock struct {
	directory string
	guard     *os.File
	owner     ownerMetadata
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	stopTimer func()

	mu           sync.Mutex
	heartbeatErr error
}

func acquireWriterLock(path string) (*writerLock, error) {
	ticker := time.NewTicker(writerHeartbeatInterval)
	lock, err := acquireWriterLockWithConfig(path, lockConfig{
		now:       time.Now,
		heartbeat: ticker.C,
	})
	if err != nil {
		ticker.Stop()
		return nil, err
	}
	lock.stopTimer = ticker.Stop
	return lock, nil
}

func acquireWriterLockWithConfig(path string, config lockConfig) (_ *writerLock, returnErr error) {
	if config.now == nil {
		config.now = time.Now
	}
	if err := ensureLockDirectory(path); err != nil {
		return nil, err
	}
	guard, err := openWriterGuard(filepath.Join(path, lockGuardName))
	if err != nil {
		return nil, err
	}
	defer func() {
		if returnErr != nil {
			returnErr = errors.Join(returnErr, closeWriterGuard(guard))
		}
	}()

	now := config.now().UTC()
	existing, found, err := readOwnerMetadata(filepath.Join(path, lockOwnerName))
	if err != nil {
		return nil, err
	}
	if found {
		age := now.Sub(existing.HeartbeatAt)
		if age < 0 || age <= writerStaleAfter {
			return nil, ErrWriterLocked
		}
	}
	sessionID, err := newSessionID()
	if err != nil {
		return nil, err
	}
	lock := &writerLock{
		directory: path,
		guard:     guard,
		owner: ownerMetadata{
			PID:         os.Getpid(),
			SessionID:   sessionID,
			StartedAt:   now,
			HeartbeatAt: now,
		},
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if err := lock.writeOwner(); err != nil {
		return nil, err
	}
	if config.heartbeat == nil {
		close(lock.done)
	} else {
		go lock.heartbeatLoop(config.heartbeat, config.afterHeartbeat)
	}
	return lock, nil
}

func ensureLockDirectory(path string) error {
	if err := os.Mkdir(path, 0o700); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create writer lock directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect writer lock directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("writer lock path is not a directory")
	}
	return nil
}

func newSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate writer session id: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func readOwnerMetadata(path string) (ownerMetadata, bool, error) {
	encoded, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ownerMetadata{}, false, nil
	}
	if err != nil {
		return ownerMetadata{}, false, fmt.Errorf("read writer owner metadata: %w", err)
	}
	var owner ownerMetadata
	if err := json.Unmarshal(encoded, &owner); err != nil {
		return ownerMetadata{}, false, fmt.Errorf("%w: decode owner metadata: %v", ErrOwnerMetadata, err)
	}
	if owner.PID <= 0 || len(owner.SessionID) != 32 ||
		owner.StartedAt.IsZero() || owner.HeartbeatAt.IsZero() ||
		owner.HeartbeatAt.Before(owner.StartedAt) {
		return ownerMetadata{}, false, ErrOwnerMetadata
	}
	if _, err := hex.DecodeString(owner.SessionID); err != nil {
		return ownerMetadata{}, false, fmt.Errorf("%w: session id: %v", ErrOwnerMetadata, err)
	}
	return owner, true, nil
}

func (lock *writerLock) heartbeatLoop(heartbeat <-chan time.Time, afterHeartbeat func()) {
	defer close(lock.done)
	for {
		select {
		case <-lock.stop:
			return
		case tick, ok := <-heartbeat:
			if !ok {
				return
			}
			tick = tick.UTC()
			if tick.After(lock.owner.HeartbeatAt) {
				lock.owner.HeartbeatAt = tick
			}
			if err := lock.writeOwner(); err != nil {
				lock.mu.Lock()
				lock.heartbeatErr = fmt.Errorf("%w: %v", ErrWriterOwnershipLost, err)
				lock.mu.Unlock()
				return
			}
			if afterHeartbeat != nil {
				afterHeartbeat()
			}
		}
	}
}

func (lock *writerLock) writeOwner() error {
	encoded, err := json.Marshal(lock.owner)
	if err != nil {
		return fmt.Errorf("encode writer owner metadata: %w", err)
	}
	encoded = append(encoded, '\n')
	path := filepath.Join(lock.directory, lockOwnerName)
	if err := writeAtomic(path, encoded, false, nil); err != nil {
		return fmt.Errorf("write writer owner metadata: %w", err)
	}
	return nil
}

func (lock *writerLock) checkHealthy() error {
	lock.mu.Lock()
	defer lock.mu.Unlock()
	return lock.heartbeatErr
}

func (lock *writerLock) release() error {
	lock.stopOnce.Do(func() {
		close(lock.stop)
	})
	<-lock.done
	if lock.stopTimer != nil {
		lock.stopTimer()
	}

	var result error
	if err := lock.checkHealthy(); err != nil {
		result = errors.Join(result, err)
	}
	owner, found, err := readOwnerMetadata(filepath.Join(lock.directory, lockOwnerName))
	if err != nil {
		result = errors.Join(result, err)
	} else if found && owner.SessionID == lock.owner.SessionID {
		if err := os.Remove(filepath.Join(lock.directory, lockOwnerName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, fmt.Errorf("remove writer owner metadata: %w", err))
		}
	}
	return errors.Join(result, closeWriterGuard(lock.guard))
}
