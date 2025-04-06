package storage

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/hoyle1974/temporal/misc"
)

type memoryStorage struct {
	_    misc.NoCopy
	lock sync.Mutex
	data map[string][]byte
}

func NewMemoryStorage() *memoryStorage {
	return &memoryStorage{data: make(map[string][]byte)}
}

func (m *memoryStorage) GetKeysWithPrefix(ctx context.Context, prefix string) ([]string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	ret := []string{}

	for k, _ := range m.data {
		if strings.HasPrefix(k, prefix) {
			ret = append(ret, k)
		}
	}

	return ret, nil
}

func (m *memoryStorage) Write(ctx context.Context, key string, data []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.data[key] = data

	return nil
}

type memoryStreamWriter struct {
	storage *memoryStorage
	key     string
}

func (m *memoryStreamWriter) Write(data []byte) (int, error) {
	m.storage.lock.Lock()
	defer m.storage.lock.Unlock()

	ol := len(data)

	if cur, ok := m.storage.data[m.key]; ok {
		data = append(cur, data...)
	}

	m.storage.data[m.key] = data

	return ol, nil
}

func (m *memoryStreamWriter) Close() error {
	return nil
}

func (s *memoryStorage) BeginStream(ctx context.Context, key string) StreamWriter {
	return &memoryStreamWriter{
		storage: s,
		key:     key,
	}
}

// ReadFile reads data from a file for a given timestamp and granularity
func (m *memoryStorage) Read(ctx context.Context, key string) ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	data, ok := m.data[key]
	if !ok {
		return data, os.ErrNotExist
	}

	return data, nil
}

// DeleteFile deletes a file for a given timestamp and granularity
func (m *memoryStorage) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.data, key)

	return nil
}
