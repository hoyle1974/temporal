package events

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"time"

	"github.com/hoyle1974/temporal/chunks"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
)

const layout = "20060102_150405.000000000"

type Sink interface {
	Append(event Event) (bool, error)
}

type Index interface {
	GetStateAt(timestamp time.Time) (map[string][]byte, error)
	UpdateIndex(header chunks.Header) error
}

type sink struct {
	key               string
	store             storage.System
	index             Index
	totalBytesWritten int64
	writer            storage.StreamWriter
	maxSinkSize       int64
}

// Append implements Sink.
func (s *sink) Append(event Event) (bool, error) {
	b, err := misc.EncodeToBytes(event)
	if err != nil {
		return false, err
	}
	value := uint32(len(b))
	err = binary.Write(s.writer, binary.BigEndian, value)
	if err != nil {
		return false, err
	}
	bytesWritten, err := s.writer.Write(b)
	if err != nil {
		return false, err
	}
	if bytesWritten != len(b) {
		return false, errors.New("could not write all data to the file")
	}

	s.totalBytesWritten += int64(bytesWritten)

	if s.totalBytesWritten > s.maxSinkSize {
		// Chunk this and start a new event stream
		err = s.FlushSink(event.Timestamp)
		if err != nil {
			return true, err
		}
	}

	return false, nil
}

func NewSink(s storage.System, i Index, maxSinkSize int64) Sink {
	t := time.Now().UTC()

	formatted := t.Format(layout)
	key := "events/" + formatted + ".events"

	writer := s.BeginStream(context.Background(), key)

	return &sink{key: key, writer: writer, store: s, index: i, maxSinkSize: maxSinkSize}
}

func (s *sink) FlushSink(timestamp time.Time) error {
	err := s.writer.Close()
	if err != nil {
		return err
	}

	err = processOldSinks(s.store, s.index, []string{s.key})
	if err != nil {
		return err
	}

	formatted := timestamp.UTC().Add(time.Nanosecond).Format(layout)
	key := "events/" + formatted + ".events"
	s.writer = s.store.BeginStream(context.Background(), key)
	s.totalBytesWritten = 0
	s.key = key

	return nil
}

func ProcessOldSinks(s storage.System, index Index) error {
	// Read any old log sinks, clean them up and store
	// them as chunks
	keys, err := s.GetKeysWithPrefix(context.Background(), "events/")
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}

	return processOldSinks(s, index, keys)
}

func processOldSinks(s storage.System, index Index, keys []string) error {

	var events []Event

	for _, key := range keys {
		data, err := s.Read(context.Background(), key)
		if err != nil {
			return err
		}
		reader := bytes.NewReader(data)

		for reader.Len() > 0 {
			var length uint32

			// Read the length (first 4 bytes)
			if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
				return err
			}

			// Read the actual data of 'length' bytes
			content := make([]byte, length)
			if _, err := reader.Read(content); err != nil {
				return err
			}

			var e Event
			err := misc.DecodeFromBytes(content, &e)
			if err != nil {
				return err
			}
			events = append(events, e)
		}
	}

	if len(events) > 0 {

		// Sort all the events
		sort.Slice(events, func(i, j int) bool {
			return events[i].Timestamp.Before(events[j].Timestamp)
		})

		state := map[string][]byte{}

		// Find the events that make up the initial keyframe, they will have the same timestamp
		// as the first event
		start := events[0].Timestamp
		buildKeyframe := true

		chunk := chunks.NewChunk(start)
		var keyFrame chunks.KeyFrame
		toFinish := []chunks.Event{}
		for _, e := range events {
			if buildKeyframe && e.Timestamp != start {
				buildKeyframe = false
				keyFrame = chunks.NewKeyFrame(state)
			}
			if e.Timestamp == start {
				e.Apply(state)
			} else {
				toFinish = append(toFinish, chunks.Event{
					Timestamp: e.Timestamp,
					Key:       e.Key,
					Data:      e.Data,
					Delete:    e.Delete,
				})
			}
		}
		if buildKeyframe {
			keyFrame = chunks.NewKeyFrame(state)
		}
		chunk.Finish(keyFrame, toFinish)

		err := chunk.Save(context.Background(), s)
		if err != nil {
			return err
		}

		err = index.UpdateIndex(chunk.Header)
		if err != nil {
			return err
		}
	}

	for _, key := range keys {
		s.Delete(context.Background(), key)
	}

	return nil
}
