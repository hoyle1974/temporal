package temporal

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/hoyle1974/temporal/chunks"
	"github.com/hoyle1974/temporal/events"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
	"github.com/hoyle1974/temporal/telemetry"
	"github.com/hoyle1974/temporal/temporal"
)

// Writes can only occur at the same time or after previosu writes
// you can't go back in time to do writes with this system
type Write interface {
	Set(ctx context.Context, timestamp time.Time, key string, data []byte) error
	Del(ctx context.Context, timestamp time.Time, key string) error
}

// Reads can read from any point in time
type Read interface {
	Get(ctx context.Context, timestamp time.Time, key string) ([]byte, error)
	GetAll(ctx context.Context, timestamp time.Time) (map[string][]byte, error)
}

type Meta interface {
	GetMinTime() time.Time
	GetMaxTime() time.Time
	GetMinMaxTime() (time.Time, time.Time)
}

type ReadWriteMap interface {
	Write
	Read
	Meta
}

/*
We want to store data in S3

We will write immediate changes as event logs, keep track of how much we have written.  When we
have written 8mb, we start a new file.
*/
type temporalMap struct {
	lock      sync.Mutex
	index     chunks.Index
	storage   storage.System
	eventMap  temporal.Map
	eventSink events.Sink
	current   time.Time
	data      map[string][]byte
	minTime   time.Time
}

type MapConfig struct {
	MaxChunkTargetSize int64
	MaxChunkAge        time.Duration
	Metrics            telemetry.Metrics
	Logger             telemetry.Logger
}

func NewMap(storage storage.System) (ReadWriteMap, error) {
	return NewMapWithConfig(storage, MapConfig{MaxChunkTargetSize: 8 * 1024 * 1024})
}

func NewMapWithConfig(storage storage.System, config MapConfig) (ReadWriteMap, error) {
	if config.Logger == nil {
		config.Logger = telemetry.NOPLogger{}
	}
	if config.Metrics == nil {
		config.Metrics = telemetry.NOPMetrics{}
	}

	// Build/Load indexes
	index, err := chunks.NewChunkIndex(storage, config.MaxChunkAge, config.Logger, config.Metrics)
	if err != nil {
		return nil, errors.Wrap(err, "could not create index")
	}

	// Load current events in the event synk
	err = events.ProcessOldSinks(config.Logger, storage, index)
	if err != nil {
		return nil, errors.Wrap(err, "could not process old sinks")
	}

	keys, err := index.GetStateAt(time.Now())
	if err != nil {
		return nil, errors.Wrap(err, "could not get state at time")
	}

	return &temporalMap{
		storage:   storage,
		index:     index,
		eventSink: events.NewSink(storage, index, config.MaxChunkTargetSize, config.MaxChunkAge, config.Logger, config.Metrics),
		data:      keys,
		current:   time.Now(),
		minTime:   index.GetMinTime(),
		eventMap:  temporal.New(),
	}, nil
}

// Get implements ReadWriteMap.
func (t *temporalMap) Get(ctx context.Context, timestamp time.Time, key string) ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if timestamp.IsZero() || !timestamp.Before(t.current) {
		return t.data[key], nil
	}

	// TODO might we be looking at unindex data?
	min, max := t.eventMap.GetTimeRange()
	if (min.Equal(timestamp) || timestamp.After(min)) && (max.Equal(timestamp) || timestamp.Before(max)) {
		data := t.eventMap.GetItem(timestamp, key)
		return data, nil
	}

	state, err := t.index.GetStateAt(timestamp)
	if err != nil {
		return nil, errors.Wrap(err, "can not get state at time")
	}

	return state[key], nil
}

// GetAll implements ReadWriteMap.
func (t *temporalMap) GetAll(ctx context.Context, timestamp time.Time) (map[string][]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if timestamp.IsZero() || !timestamp.Before(t.current) {
		return misc.DeepCopyMap(t.data), nil
	}

	min, max := t.eventMap.GetTimeRange()
	if (min.Equal(timestamp) || timestamp.After(min)) && (max.Equal(timestamp) || timestamp.Before(max)) {
		state := t.eventMap.GetStateAtTime(timestamp)
		return state, nil
	}

	state, err := t.index.GetStateAt(timestamp)
	if err != nil {
		return nil, errors.Wrap(err, "can not get state at time")
	}

	return state, nil

}

// Set implements ReadWriteMap.
func (t *temporalMap) Set(ctx context.Context, timestamp time.Time, key string, data []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.current.IsZero() {
		t.current = timestamp
	}
	if timestamp.Before(t.current) {
		return errors.New("del: cannot delete data from the past")
	}

	flushed, err := t.eventSink.Append(events.Event{
		Timestamp: timestamp,
		Key:       key,
		Data:      data,
		Delete:    false,
	})
	if err != nil {
		return err
	}
	if flushed {
		t.eventMap = temporal.New()
	}

	t.data[key] = data
	t.current = timestamp
	t.eventMap.Add(timestamp, key, data)

	return nil
}

// Set implements ReadWriteMap.
func (t *temporalMap) Del(ctx context.Context, timestamp time.Time, key string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.current.IsZero() {
		t.current = timestamp
	}
	if timestamp.Before(t.current) {
		return errors.New("del: timestamp is before current")
	}

	flushed, err := t.eventSink.Append(events.Event{
		Timestamp: timestamp,
		Key:       key,
		Delete:    true,
	})
	if err != nil {
		return err
	}
	if flushed {
		t.eventMap = temporal.New()
	}

	delete(t.data, key)
	t.current = timestamp
	t.eventMap.Remove(timestamp, key)

	return nil
}

func (t *temporalMap) GetMinTime() time.Time {
	return t.minTime
}
func (t *temporalMap) GetMaxTime() time.Time {
	return t.current
}
func (t *temporalMap) GetMinMaxTime() (time.Time, time.Time) {
	return t.minTime, t.current
}
