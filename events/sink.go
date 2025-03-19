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

type Sink interface {
	Append(event Event) error
}

type Index interface {
	GetStateAt(timestamp time.Time) (map[string][]byte, error)
	UpdateIndex(header chunks.Header) error
}

type sink struct {
	writer storage.StreamWriter
}

// Append implements Sink.
func (s *sink) Append(event Event) error {
	b, err := misc.EncodeToBytes(event)
	if err != nil {
		return err
	}
	value := uint32(len(b))
	err = binary.Write(s.writer, binary.BigEndian, value)
	if err != nil {
		return err
	}
	bytesWritten, err := s.writer.Write(b)
	if err != nil {
		return err
	}
	if bytesWritten != len(b) {
		return errors.New("could not write all data to the file")
	}

	return nil
}

func NewSink(s storage.System) Sink {
	t := time.Now().UTC()
	formatted := t.Format("20060102_150405_000000000")
	key := "events/" + formatted + ".events"

	writer := s.BeginStream(context.Background(), key)

	return &sink{writer: writer}
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
		for _, e := range events {
			if buildKeyframe && e.Timestamp != start {
				buildKeyframe = false
				chunk.SetKeyframe(state)
			}
			if e.Timestamp == start {
				e.Apply(state)
			} else {
				chunk.ApplyEvent(chunks.Event{
					Timestamp: e.Timestamp,
					Key:       e.Key,
					Data:      e.Data,
					Delete:    e.Delete,
				})
			}
		}
		if buildKeyframe {
			chunk.SetKeyframe(state)
		}

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

type Event struct {
	Timestamp time.Time
	Key       string
	Data      []byte
	Delete    bool
}

func (e Event) Apply(ret map[string][]byte) {
	if e.Delete {
		delete(ret, e.Key)
	} else {
		ret[e.Key] = e.Data
	}
}
