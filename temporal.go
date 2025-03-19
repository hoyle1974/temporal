package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
	"github.com/patrickmn/go-cache"
)

type Config struct {
	MaxFileSize   int
	MaxDiffFrames int
}

func DefaultConfig() Config {
	return Config{
		MaxFileSize:   8 * 1024 * 1024,
		MaxDiffFrames: 599,
	}
}

type StorageManager struct {
	storage      storage.System
	lock         sync.Mutex
	lastWrite    time.Time
	current      collated
	lastFrame    Keyframe
	currentSize  atomic.Int64
	minTime      time.Time
	maxTime      time.Time
	maxFileSize  int
	maxDiffFrame int
	chunkCache   *cache.Cache
	pool         pond.Pool
}

func NewStorageManager(storage storage.System, config ...Config) (*StorageManager, error) {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	sm := &StorageManager{
		storage:      storage,
		maxFileSize:  cfg.MaxFileSize,
		maxDiffFrame: cfg.MaxDiffFrames,
		chunkCache:   cache.New(5*time.Minute, 10*time.Minute),
		pool:         pond.NewPool(16),
	}

	startBin, err := storage.Read(context.Background(), "start.idx")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if startBin != nil {
		startTimeKey := string(startBin)
		layout := "20060102_150405"

		t, err := time.Parse(layout, startTimeKey)
		if err != nil {
			return nil, err
		}
		t = t.UTC()

		sm.minTime = t
	}
	sm.maxTime = time.Now().UTC().Truncate(time.Second)

	return sm, nil
}

func (s *StorageManager) PrintStats() {
	s.ReadData(context.Background(), s.minTime)

	timestamp := s.minTime
	key := fmt.Sprintf("%04d/%02d/%02d/%s.chunk", timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Format("20060102_150405"))

	if val, ok := s.chunkCache.Get(key); ok {
		val.(*collated).PrintStats()
	}
}

func (s *StorageManager) BeginIngest(dataIngest func() []byte) {
	if dataIngest == nil { // No data ingest function
		return
	}

	ticker := time.NewTicker(1 * time.Second)

	t := time.Now().Truncate(time.Second)
	for range ticker.C {
		data := dataIngest()
		s.WriteData(context.TODO(), t, data)
		t = t.Add(1 * time.Second)
	}

}

func (s *StorageManager) GetMinMaxTime() (time.Time, time.Time) {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.minTime, s.maxTime
}

func (s *StorageManager) Flush(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.flush(ctx)
}

func (s *StorageManager) flush(ctx context.Context) error {
	// Write this and create a new keyframe
	key := fmt.Sprintf("%04d/%02d/%02d/%s.chunk", s.current.Timestamp.Year(), s.current.Timestamp.Month(), s.current.Timestamp.Day(), s.current.Timestamp.Format("20060102_150405"))

	s.pool.StopAndWait()
	s.pool = pond.NewPool(16)

	// Wrap everything with a cache
	for idx, d := range s.current.Diffs {
		s.current.Diffs[idx] = UnwrapCache(d)
	}
	b, err := misc.EncodeToBytes(s.current)
	if err != nil {
		return err
	}

	err = s.storage.Write(ctx, key, b)
	if err != nil {
		return err
	}

	s.current = collated{}
	s.currentSize.Swap(0)

	return nil
}

// Writes data with a timestamp to the store.  Each write must have a timestamp that is increasing
// by a second from the previous
func (s *StorageManager) WriteData(ctx context.Context, timestamp time.Time, data []byte) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	timestamp = timestamp.UTC()

	timestamp = timestamp.Truncate(time.Second)
	s.maxTime = timestamp

	if s.minTime.IsZero() {
		s.minTime = timestamp
		key := timestamp.Format("20060102_150405")
		err := s.storage.Write(ctx, "start.idx", []byte(key))
		if err != nil {
			return err
		}
	}

	if s.current.Timestamp.IsZero() {
		// This is the start of writing data, so we will take this as a keyframe
		s.current.Timestamp = timestamp
		s.current.Keyframe = data
		s.current.Diffs = []DiffFrame{}
		s.lastWrite = timestamp
		s.lastFrame = s.current.Keyframe
		s.currentSize.Add(int64(len(data)))
		return nil
	}

	d := timestamp.Sub(s.lastWrite)
	if d < time.Second {
		return fmt.Errorf("not enough time has passed to write new data: %v", d)
	}

	idx := len(s.current.Diffs)
	diff1 := NewDiffFrame1(data)
	s.current.Diffs = append(s.current.Diffs, &diff1)

	diff1.Convert(s.pool, idx, &s.current, s.lastFrame)
	s.lastFrame = data
	s.lastWrite = timestamp

	if int(s.currentSize.Load()) > s.maxFileSize || len(s.current.Diffs) > s.maxDiffFrame {
		// Write this and create a new keyframe
		return s.flush(ctx)
	}

	return nil
}

func (s *StorageManager) ReadData(ctx context.Context, timestamp time.Time) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// Convert the timestamp UTC for sanity and truncated at the second
	timestamp = timestamp.UTC().Truncate(time.Second)
	originalTimestamp := timestamp

	// Make a copy of the original timestamp, this one we will modify
	origTruncKey := fmt.Sprintf("%04d/%02d/%02d/%s.chunk", timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Format("20060102_150405"))

	if !s.current.Timestamp.IsZero() && (timestamp.Equal(s.current.Timestamp) || timestamp.After(s.current.Timestamp)) {
		return s.current.getFrame(timestamp)
	}

	for {
		// The key to look for the chunk
		truncKey := fmt.Sprintf("%04d/%02d/%02d/%s.chunk", timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Format("20060102_150405"))

		// chunk cache
		if cached, ok := s.chunkCache.Get(truncKey); ok {
			// Make a note we found the chunk for 'origTimestamp' at 'timestamp'
			c := cached.(*collated)
			s.chunkCache.Set(origTruncKey, c, time.Minute)
			return c.getFrame(originalTimestamp)
		}

		// Read the key from disk
		data, _ := s.storage.Read(ctx, truncKey)
		if data != nil {
			var c collated
			err := misc.DecodeFromBytes(data, &c)
			if err != nil {
				return nil, err
			}
			// Wrap everything with a cache
			for idx, d := range c.Diffs {
				c.Diffs[idx] = WrapWithCache(d, nil)
			}
			go func() {
				// Iterate and cache the data
				k := c.Keyframe
				for _, d := range c.Diffs {
					k = d.GetKeyframe(k)
				}
			}()
			// Cache the decoded structure for future use
			s.chunkCache.Set(origTruncKey, c, time.Minute)
			return c.getFrame(originalTimestamp)
		}

		// Go back in time 1 second and try again
		timestamp = timestamp.Add(-time.Second)
	}

}
