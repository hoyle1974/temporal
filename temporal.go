package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type StorageManager struct {
	storage     Storage
	lock        sync.Mutex
	lastWrite   time.Time
	current     collated
	lastFrame   Keyframe
	currentSize int
	minTime     time.Time
	maxTime     time.Time
}

func NewStorageManager(storage Storage) (*StorageManager, error) {
	sm := &StorageManager{
		storage: storage,
	}

	startBin, err := storage.Read(context.Background(), "start.idx")
	if err != nil {
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
		s.current.Diffs = []Diff{}
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
	s.current.Diffs = append(s.current.Diffs, diff)
	s.currentSize += len(diff)
	s.lastWrite = timestamp

	if s.currentSize > 8*1024*1024 || len(s.current.Diffs) > 599 {
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

	if trunc.Equal(s.current.Timestamp) || trunc.After(s.current.Timestamp) {
		return s.current.getFrame(timestamp)
	}

	for {
		truncKey := fmt.Sprintf("%04d/%02d/%02d/%s.chunk",
			trunc.Year(),
			trunc.Month(),
			trunc.Day(),
			trunc.Format("20060102_150405"))

		data, _ := s.storage.Read(ctx, truncKey)
		if data != nil {
			c, err := decodeToStruct(data)
			if err != nil {
				return nil, err
			}
			return c.getFrame(timestamp)
		}
		trunc = trunc.Add(-time.Second)
	}

}
