package events

import (
	"context"

	"github.com/hoyle1974/temporal/storage"
)

type Meta interface {
	GetEventFiles() ([]string, error)
}

func NewMeta(storage storage.System) (Meta, error) {
	return &meta{store: storage}, nil
}

type meta struct {
	store storage.System
}

func (m *meta) GetEventFiles() ([]string, error) {
	return m.store.GetKeysWithPrefix(context.Background(), "events/")
}
