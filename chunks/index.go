package chunks

import (
	"context"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
	"github.com/hoyle1974/temporal/telemetry"
)

type Index interface {
	UpdateIndex(header Header) error
	findHeaderResponsibleFor(timestamp time.Time) (Header, error)
	GetStateAt(timestamp time.Time) (map[string][]byte, error)
	GetMinTime() time.Time
	GetMaxTime() time.Time
	GetHeaders() []Header
}

// The chunk index manages all the chunks
type index struct {
	_           misc.NoCopy
	lock        sync.Mutex
	storage     storage.System
	headers     []Header
	minTime     time.Time
	maxTime     time.Time
	maxChunkAge time.Duration
	metrics     telemetry.Metrics
	logger      telemetry.Logger
}

func NewChunkIndex(storage storage.System, maxChunkAge time.Duration, logger telemetry.Logger, metrics telemetry.Metrics) (Index, error) {
	logger.Info("NewChunkIndex")
	ci := &index{
		storage:     storage,
		headers:     []Header{},
		maxChunkAge: maxChunkAge,
		metrics:     metrics,
		logger:      logger,
	}

	// If start.idx doesn't exist, then we never started
	startBin, err := storage.Read(context.Background(), "start.idx")
	if errors.Is(err, os.ErrNotExist) {
		return ci, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return ci, errors.Wrap(err, "can not read start.idx")
	}
	if startBin != nil || len(startBin) > 0 {
		startTimeKey := string(startBin)

		t, err := time.Parse(layout, startTimeKey)
		if err != nil {
			return ci, errors.Wrap(err, "can not parse start.idx")
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
			return ci, errors.Wrap(err, "can not load header")
		}
		ci.headers = append(ci.headers, h)
		currChunkId = h.Next
		ci.adjustMinMax(h.Min)
		ci.adjustMinMax(h.Max)
	}

	return ci, nil
}

func (ci *index) GetHeaders() []Header {
	return ci.headers
}

func (ci *index) UpdateIndex(header Header) error {
	ci.lock.Lock()
	defer ci.lock.Unlock()

	if ci.minTime.IsZero() {
		ci.adjustMinMax(header.Min)
		ci.adjustMinMax(header.Max)
		err := ci.storage.Write(context.Background(), "start.idx", []byte(ci.minTime.UTC().Format(layout)))
		if err != nil {
			return errors.Wrap(err, "can not write start.idx")
		}
	}

	ci.headers = append(ci.headers, header)

	// Sort all the events
	sort.Slice(ci.headers, func(i, j int) bool {
		return ci.headers[i].Min.Before(ci.headers[j].Min)
	})

	// Cleanup old headers & chunks
	if ci.maxChunkAge != time.Duration(0) {
		for _, header := range ci.headers {
			ci.adjustMinMax(header.Min)
			ci.adjustMinMax(header.Max)
		}
		minValidTime := ci.maxTime.Add(-ci.maxChunkAge)
		startIdx := 0
		for idx := range ci.headers {
			if ci.headers[idx].Max.Before(minValidTime) {
				ci.headers[idx].RemoveFromStorage(context.Background(), ci.storage)
				startIdx = idx + 1
				break
			}
		}
		if startIdx != 0 {
			ci.headers = ci.headers[startIdx+1:]
		}
	}

	// Reset and recalculate
	ci.minTime = time.Time{}
	ci.maxTime = time.Time{}

	// Stich them together if needed
	modified := map[int]any{}
	for idx, _ := range ci.headers {
		ci.adjustMinMax(ci.headers[idx].Min)
		ci.adjustMinMax(ci.headers[idx].Max)

		if ci.headers[idx].Next == "" && idx+1 < len(ci.headers) {
			ci.headers[idx].Next = ci.headers[idx+1].Id
			modified[idx] = true
		}
		if ci.headers[idx].Prev == "" && idx > 0 {
			ci.headers[idx].Prev = ci.headers[idx-1].Id
			modified[idx] = true
		}
		if idx == 0 && ci.headers[idx].Prev != "" { // Make sure our first one isn't pointing back
			ci.headers[idx].Prev = ""
			modified[idx] = true
		}
	}
	for idx := range modified {
		err := ci.headers[idx].Save(context.Background(), ci.storage)
		if err != nil {
			return errors.Wrap(err, "can not save header")
		}
	}

	return nil
}

func (ci *index) GetMinTime() time.Time {
	return ci.minTime
}

func (ci *index) GetMaxTime() time.Time {
	return ci.maxTime
}

func (ci *index) adjustMinMax(a time.Time) {
	if ci.minTime.IsZero() {
		ci.minTime = a
	}
	if ci.maxTime.IsZero() {
		ci.maxTime = a
	}
	if a.Before(ci.minTime) {
		ci.minTime = a
	}
	if ci.maxTime.Before(a) {
		ci.maxTime = a
	}
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
		return nil, errors.Wrap(err, "can not find header")
	}

	chunk, err := header.LoadChunk(context.Background(), ci.storage)
	if err != nil {
		return nil, errors.Wrap(err, "can not load chunk")
	}

	return chunk.GetStateAt(timestamp)
}
