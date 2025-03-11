package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

type DiskStorage struct {
	BaseDir string
}

// NewDiskStorage initializes a new DiskStorage instance
func NewDiskStorage(baseDir string) *DiskStorage {
	return &DiskStorage{BaseDir: baseDir}
}

// Write writes data to a file for a given key
func (ds *DiskStorage) Write(ctx context.Context, key string, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	filePath := filepath.Join(ds.BaseDir, key)
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(filePath, data, 0644)
}

// Read reads data from a file for a given key
func (ds *DiskStorage) Read(ctx context.Context, key string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	filePath := filepath.Join(ds.BaseDir, key)
	return os.ReadFile(filePath)
}

// Delete deletes a file for a given key
func (ds *DiskStorage) Delete(ctx context.Context, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	filePath := filepath.Join(ds.BaseDir, key)
	if err := os.Remove(filePath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // Ignore file not found errors
		}
		return err
	}
	return nil
}
