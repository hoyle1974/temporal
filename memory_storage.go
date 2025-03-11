package temporal

import (
	"context"
	"sync"
)

type MemoryStorage struct {
	lock sync.Mutex
	data map[string][]byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{data: make(map[string][]byte)}
}

func (m *MemoryStorage) Write(ctx context.Context, key string, data []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.data[key] = data

	return nil
}

// ReadFile reads data from a file for a given timestamp and granularity
func (m *MemoryStorage) Read(ctx context.Context, key string) ([]byte, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	data, _ := m.data[key]

	return data, nil
}

// DeleteFile deletes a file for a given timestamp and granularity
func (m *MemoryStorage) Delete(ctx context.Context, key string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.data, key)

	return nil
}
