package chunks

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/hoyle1974/temporal/storage"
)

// The chunk index manages all the chunks
type Index struct {
	lock    sync.Mutex
	storage storage.System
	headers []Header
	minTime time.Time
}

func NewChunkIndex(storage storage.System) (Index, error) {

	ci := Index{storage: storage, headers: []Header{}}

	// If start.idx doesn't exist, then we never started
	startBin, err := storage.Read(context.Background(), "start.idx")
	if errors.Is(err, os.ErrNotExist) {
		return ci, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ci, err
	}
	if startBin != nil || len(startBin) > 0 {
		startTimeKey := string(startBin)

		t, err := time.Parse(layout, startTimeKey)
		if err != nil {
			return ci, err
		}
		t = t.UTC()

		ci.minTime = t
	} else {
		return ci, errors.New("invalid start.idx")
	}

	// Load all headers
	currChunkId := NewChunkId(ci.minTime)
	for currChunkId != "" {
		h, err := LoadHeader(context.Background(), storage, currChunkId)
		if err != nil {
			return ci, err
		}
		ci.headers = append(ci.headers, h)
		currChunkId = h.Next
	}

	return ci, nil
}

func (ci *Index) UpdateIndex(header Header) error {
	ci.lock.Lock()
	defer ci.lock.Unlock()

	if ci.minTime.IsZero() {
		ci.minTime = header.Min
		err := ci.storage.Write(context.Background(), "start.idx", []byte(ci.minTime.UTC().Format(layout)))
		if err != nil {
			return err
		}
	}

	ci.headers = append(ci.headers, header)

	return nil
}

func (ci *Index) findHeaderResponsibleFor(timestamp time.Time) (Header, error) {
	ci.lock.Lock()
	defer ci.lock.Unlock()

	if len(ci.headers) == 0 {
		return Header{}, nil
	}
	if timestamp.After(ci.headers[len(ci.headers)-1].Min) {
		return ci.headers[len(ci.headers)-1], nil
	}

	for _, h := range ci.headers {
		if h.ResponsibleFor(timestamp) {
			return h, nil
		}
	}

	return Header{}, errors.New("no header found")
}

func (ci *Index) GetStateAt(timestamp time.Time) (map[string][]byte, error) {
	if ci.minTime.IsZero() {
		return map[string][]byte{}, nil
	}
	if timestamp.Before(ci.minTime) {
		return map[string][]byte{}, nil
	}

	// Find the chunk that would have time.Time in it
	header, err := ci.findHeaderResponsibleFor(timestamp)
	if err != nil {
		return nil, err
	}

	chunk, err := header.LoadChunk(context.Background(), ci.storage)
	if err != nil {
		return nil, err
	}

	return chunk.GetStateAt(timestamp)
}
