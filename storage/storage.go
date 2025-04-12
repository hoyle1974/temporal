package storage

import (
	"context"
	"errors"
	"io"
)

type StreamWriter interface {
	io.Writer
	io.Closer
}

var ErrDoesNotExist = errors.New("does not exist")

// System defines the operations for interacting with the storage backend
type System interface {
	// WriteFile writes data to a file for a given timestamp and granularity (e.g., second, minute, hour)
	Write(ctx context.Context, key string, data []byte) error

	BeginStream(ctx context.Context, key string) StreamWriter

	// ReadFile reads data from a file for a given timestamp and granularity
	Read(ctx context.Context, key string) ([]byte, error)

	// DeleteFile deletes a file for a given timestamp and granularity
	Delete(ctx context.Context, key string) error

	GetKeysWithPrefix(ctx context.Context, prefix string) ([]string, error)
}
