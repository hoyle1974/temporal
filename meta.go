package temporal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hoyle1974/temporal/chunks"
	"github.com/hoyle1974/temporal/events"
	"github.com/hoyle1974/temporal/storage"
)

type temporalMeta struct {
	lock     sync.Mutex
	index    chunks.Index
	sinkMeta events.Meta
	storage  storage.System
}

func NewMeta(storage storage.System) (Meta, error) {

	sinkMeta, err := events.NewMeta(storage)
	if err != nil {
		return nil, err
	}

	// Build/Load indexes
	index, err := chunks.NewChunkIndex(storage, time.Duration(0))
	if err != nil {
		return nil, err
	}

	t := &temporalMeta{
		storage:  storage,
		index:    index,
		sinkMeta: sinkMeta,
	}

	fmt.Println("Events")
	keys, _ := sinkMeta.GetEventFiles()
	for _, k := range keys {
		events, _ := events.GetEvents(storage, k)
		fmt.Printf("	%s:%d events\n", k, len(events))
	}

	fmt.Println("Chunks")
	for _, h := range index.GetHeaders() {
		chunk, _ := h.LoadChunk(context.Background(), storage)
		fmt.Printf("	[%v - %v] Disk Size:%d  Keys:%d  Events:%d   RawSize:%d  Ratio:%f\n",
			h.Min.UTC(),
			h.Max.UTC(),
			chunk.Data.GetDiskSize(),
			len(chunk.Data.Keys),
			len(chunk.Data.Diffs),
			chunk.Data.RawSize(),
			float64(chunk.Data.RawSize())/float64(chunk.Data.GetDiskSize()),
		)
	}

	return t, nil
}

func (m *temporalMeta) GetMinTime() time.Time {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.index.GetMinTime()
}
func (m *temporalMeta) GetMaxTime() time.Time {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.index.GetMaxTime()
}

func (m *temporalMeta) GetMinMaxTime() (time.Time, time.Time) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.index.GetMinTime(), m.index.GetMaxTime()
}
