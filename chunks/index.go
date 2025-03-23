package chunks

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hoyle1974/temporal/storage"
)

type Index interface {
	UpdateIndex(header Header) error
	findHeaderResponsibleFor(timestamp time.Time) (Header, error)
	GetStateAt(timestamp time.Time) (map[string][]byte, error)
	GetMinTime() time.Time
}

// The chunk index manages all the chunks
type index struct {
	lock    sync.Mutex
	storage storage.System
	headers []Header
	minTime time.Time
}

func NewChunkIndex(storage storage.System) (Index, error) {

	ci := &index{storage: storage, headers: []Header{}}

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

func (ci *index) UpdateIndex(header Header) error {
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

	// Sort all the events
	sort.Slice(ci.headers, func(i, j int) bool {
		return ci.headers[i].Min.Before(ci.headers[j].Min)
	})

	// Stich them together if needed
	modified := map[int]any{}
	for idx, _ := range ci.headers {
		if ci.headers[idx].Next == "" && idx+1 < len(ci.headers) {
			ci.headers[idx].Next = ci.headers[idx+1].Id
			modified[idx] = true
		}
		if ci.headers[idx].Prev == "" && idx > 0 {
			ci.headers[idx].Prev = ci.headers[idx-1].Id
			modified[idx] = true
		}
	}
	for idx, _ := range modified {
		err := ci.headers[idx].Save(context.Background(), ci.storage)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ci *index) GetMinTime() time.Time {
	return ci.minTime
}

func (ci *index) findHeaderResponsibleFor(timestamp time.Time) (Header, error) {
	ci.lock.Lock()
	defer ci.lock.Unlock()

	n := len(ci.headers)
	if n == 0 {
		return Header{}, nil
	}

	// Handle the case where the timestamp is after the last header's Min
	if timestamp.After(ci.headers[n-1].Min) {
		return ci.headers[n-1], nil
	}

	// Binary search
	idx := sort.Search(n-1, func(i int) bool {
		// We are looking for the first header whose Min is after or equal to the timestamp.
		// However, we need the *previous* header.
		return ci.headers[i+1].Min.After(timestamp)
	})

	if idx < 0 {
		// This should ideally not happen given the check for empty headers
		// and the check for timestamp after the last header.
		return Header{}, errors.New("no header found (index < 0)")
	}

	// Check if the found header (at index idx) is responsible
	start := ci.headers[idx].Min
	var end time.Time
	if idx+1 < n {
		end = ci.headers[idx+1].Min
	} else {
		// This case should have been handled by the initial 'timestamp.After' check
		return Header{}, errors.New("logic error: should not reach here for end time")
	}

	if (timestamp.After(start) || timestamp.Equal(start)) && timestamp.Before(end) {
		return ci.headers[idx], nil
	}

	// Special case for the very first header
	if idx == 0 && (timestamp.Before(ci.headers[0].Min) || timestamp.Equal(ci.headers[0].Min)) {
		return ci.headers[0], nil
	}

	return Header{}, errors.New("no header found")
}

func (ci *index) GetStateAt(timestamp time.Time) (map[string][]byte, error) {
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
