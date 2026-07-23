package fs_split

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	temporaryRandomBytes = 16
	temporaryAttempts    = 100
)

func createTemporaryFile(directory, prefix string) (*os.File, error) {
	return createTemporaryFileWithRandom(directory, prefix, rand.Reader)
}

func createTemporaryFileWithRandom(
	directory string,
	prefix string,
	random io.Reader,
) (*os.File, error) {
	if prefix == "" ||
		prefix == "." ||
		prefix == ".." ||
		filepath.Base(prefix) != prefix ||
		strings.ContainsAny(prefix, `/\`) {
		return nil, errors.New("temporary file prefix is not a safe path component")
	}
	randomBytes := make([]byte, temporaryRandomBytes)
	for range temporaryAttempts {
		if _, err := io.ReadFull(random, randomBytes); err != nil {
			return nil, fmt.Errorf("generate temporary file name: %w", err)
		}
		path := filepath.Join(directory, prefix+hex.EncodeToString(randomBytes))
		file, err := openExclusiveTemporaryFile(path)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return file, nil
	}
	return nil, errors.New("create temporary file: exhausted unique names")
}
