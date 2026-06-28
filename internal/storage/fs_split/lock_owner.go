package fs_split

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

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
