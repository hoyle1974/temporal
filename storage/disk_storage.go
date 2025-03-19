package storage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type diskStorage struct {
	BaseDir string
}

// NewDiskStorage initializes a new DiskStorage instance
func NewDiskStorage(baseDir string) *diskStorage {
	return &diskStorage{BaseDir: baseDir}
}

func (ds *diskStorage) GetKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	var matchedFiles []string

	searchPrefix := filepath.Join(ds.BaseDir, prefix)

	// Walk through all files and directories
	err := filepath.WalkDir(ds.BaseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Check if it's a file and starts with the prefix
		if !d.IsDir() && strings.HasPrefix(path, searchPrefix) {
			matchedFiles = append(matchedFiles, path[len(ds.BaseDir)+1:])
		}
		return nil
	})

	return matchedFiles, err
}

type diskStreamWriter struct {
	file *os.File
}

func (m *diskStreamWriter) Write(data []byte) (int, error) {
	return m.file.Write(data)
}

func (m *diskStreamWriter) Close() error {
	return m.file.Close()
}

func (ds *diskStorage) BeginStream(ctx context.Context, key string) StreamWriter {
	filePath := filepath.Join(ds.BaseDir, key)
	if err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return nil
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil
	}

	return &diskStreamWriter{file: file}
}

// Write writes data to a file for a given key
func (ds *diskStorage) Write(ctx context.Context, key string, data []byte) error {
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
func (ds *diskStorage) Read(ctx context.Context, key string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	filePath := filepath.Join(ds.BaseDir, key)
	return os.ReadFile(filePath)
}

// Delete deletes a file for a given key
func (ds *diskStorage) Delete(ctx context.Context, key string) error {
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
