package temporal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
	"weak"

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
	storage      Storage
	lock         sync.Mutex
	lastWrite    time.Time
	current      collated
	lastFrame    Keyframe
	currentSize  int
	minTime      time.Time
	maxTime      time.Time
	maxFileSize  int
	maxDiffFrame int
	cache1       *cache.Cache
	cache2       *cache.Cache
}

func NewStorageManager(storage Storage, config ...Config) (*StorageManager, error) {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	sm := &StorageManager{
		storage:      storage,
		maxFileSize:  cfg.MaxFileSize,
		maxDiffFrame: cfg.MaxDiffFrames,
		cache1:       cache.New(5*time.Minute, 10*time.Minute),
		cache2:       cache.New(5*time.Minute, 10*time.Minute),
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
		s.current.Diffs = []DiffAndCache{}
		s.lastWrite = timestamp
		s.lastFrame = s.current.Keyframe
		s.currentSize += len(data)
		return nil
	}

	d := timestamp.Sub(s.lastWrite)
	if d < time.Second {
		return fmt.Errorf("not enough time has passed to write new data: %v", d)
	}

	diff, err := generateDiff(s.lastFrame, data)
	if err != nil {
		return err
	}
	s.lastFrame = data

	var k Keyframe = copyBytes(data)
	s.current.Diffs = append(s.current.Diffs,
		DiffAndCache{
			Diff: diff,
			orig: weak.Make(&k),
		},
	)
	s.currentSize += len(diff)
	s.lastWrite = timestamp

	if s.currentSize > s.maxFileSize || len(s.current.Diffs) > s.maxDiffFrame {
		// Write this and create a new keyframe
		key := fmt.Sprintf("%04d/%02d/%02d/%s.chunk",
			s.current.Timestamp.Year(),
			s.current.Timestamp.Month(),
			s.current.Timestamp.Day(),
			s.current.Timestamp.Format("20060102_150405"))

		b, err := encodeToBytes(s.current)
		if err != nil {
			return err
		}

		err = s.storage.Write(ctx, key, b)
		if err != nil {
			return err
		}

		s.current = collated{}
		s.currentSize = 0
	}

	return nil
}

func (s *StorageManager) ReadData(ctx context.Context, timestamp time.Time) ([]byte, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	timestamp = timestamp.UTC()

	trunc := timestamp.Truncate(time.Second)
	origTrunc := trunc

	if trunc.Equal(s.current.Timestamp) || trunc.After(s.current.Timestamp) {
		return s.current.getFrame(timestamp)
	}

	if adjust, ok := s.cache1.Get(origTrunc.String()); ok {
		trunc = adjust.(time.Time)
	}

	for {
		truncKey := fmt.Sprintf("%04d/%02d/%02d/%s.chunk",
			trunc.Year(),
			trunc.Month(),
			trunc.Day(),
			trunc.Format("20060102_150405"))

		if cached, ok := s.cache2.Get(truncKey); ok {
			c := cached.(*collated)
			return c.getFrame(timestamp)
		}

		data, _ := s.storage.Read(ctx, truncKey)
		if data != nil {
			s.cache1.Set(origTrunc.String(), trunc, time.Hour)

			c, err := decodeToStruct(data)
			if err != nil {
				return nil, err
			}

			s.cache2.Set(truncKey, c, time.Minute)

			return c.getFrame(timestamp)
		}
		trunc = trunc.Add(-time.Second)
	}

}
