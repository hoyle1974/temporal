package temporal

import (
	"context"
)

// Storage defines the operations for interacting with the storage backend
type Storage interface {
	// WriteFile writes data to a file for a given timestamp and granularity (e.g., second, minute, hour)
	Write(ctx context.Context, key string, data []byte) error

	// ReadFile reads data from a file for a given timestamp and granularity
	Read(ctx context.Context, key string) ([]byte, error)

	// DeleteFile deletes a file for a given timestamp and granularity
	Delete(ctx context.Context, key string) error
}
