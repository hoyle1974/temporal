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
	"github.com/hoyle1974/temporal/telemetry"
)

const layout = "20060102_150405.000000000"

type Sink interface {
	Append(event Event) (bool, error)
}

type Index interface {
	GetStateAt(timestamp time.Time) (map[string][]byte, error)
	UpdateIndex(header chunks.Header) error
}

type Estimator interface {
	OnWriteData(bytesWritten int64)
	ShouldTryFlush() bool
	OnFlush(compressedSize int64, success bool)
}

type sink struct {
	key               string
	store             storage.System
	index             Index
	totalBytesWritten int64
	writer            storage.StreamWriter
	chunkTargetSize   int64
	maxChunkAge       time.Duration
	estimator         Estimator
	meta              Meta
	logger            telemetry.Logger
	metrics           telemetry.Metrics
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
	s.estimator.OnWriteData(int64(bytesWritten))
	s.totalBytesWritten += int64(bytesWritten)

	if s.estimator.ShouldTryFlush() {
		// Chunk this and start a new event stream
		err = s.FlushSink(event.Timestamp)
		if err != nil {
			return true, err
		}
	}

	return false, nil
}

func eventKey(t time.Time) string {
	formatted := t.UTC().Format(layout)
	return "events/" + formatted + ".events"
}

func NewSink(s storage.System, i Index, chunkTargetSize int64, maxChunkAge time.Duration, logger telemetry.Logger, metrics telemetry.Metrics) Sink {
	key := eventKey(time.Now().UTC())

	writer := s.BeginStream(context.Background(), key)

	meta, err := NewMeta(s)
	if err != nil {
		return nil
	}

	return &sink{
		key:             key,
		writer:          writer,
		store:           s,
		index:           i,
		estimator:       misc.NewCompressionEstimator(chunkTargetSize),
		chunkTargetSize: chunkTargetSize,
		maxChunkAge:     maxChunkAge,
		meta:            meta,
		logger:          logger,
		metrics:         metrics,
	}
}

func (s *sink) FlushSink(timestamp time.Time) error {
	// We close the event stream, because we think we have the events
	err := s.writer.Close()
	if err != nil {
		return err
	}
	// Make sure we start the stream again
	defer func() {
		key := eventKey(timestamp.UTC().Add(time.Nanosecond))
		s.writer = s.store.BeginStream(context.Background(), key)
		s.key = key
	}()

	// We may have more then 1 event file
	keys, err := s.meta.GetEventFiles()
	if err != nil {
		return err
	}

	estimatedSize, err := processOldSinks(s.store, s.index, s.chunkTargetSize, keys)
	if errors.Is(err, ErrSinkTooSmall) {
		s.estimator.OnFlush(estimatedSize, false)
		return nil // We didn't process them because they were not large enough
	}
	if err != nil {
		return err
	}
	s.estimator.OnFlush(estimatedSize, true)

	s.totalBytesWritten = 0

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

	_, err = processOldSinks(s, index, 0, keys)
	return err
}

var ErrSinkTooSmall = errors.New("sink to small")

func GetEvents(s storage.System, eventFile string) ([]Event, error) {

	// Read all the events so far
	var events []Event

	data, err := s.Read(context.Background(), eventFile)
	if err != nil {
		return events, err
	}
	reader := bytes.NewReader(data)

	for reader.Len() > 0 {
		var length uint32

		// Read the length (first 4 bytes)
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			return events, err
		}

		// Read the actual data of 'length' bytes
		content := make([]byte, length)
		if _, err := reader.Read(content); err != nil {
			return events, err
		}

		var e Event
		err := misc.DecodeFromBytes(content, &e)
		if err != nil {
			return events, err
		}
		events = append(events, e)
	}

	return events, nil
}

func processOldSinks(s storage.System, index Index, minimumChunkSize int64, keys []string) (int64, error) {

	// Read all the events so far
	var events []Event
	for _, key := range keys {
		e, err := GetEvents(s, key)
		if err != nil {
			return 0, err
		}
		events = append(events, e...)
	}

	var estimatedSize int64
	var err error
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

		estimatedSize, err = chunk.EstimateSize()
		if estimatedSize < int64(float64(minimumChunkSize)*0.9) || err != nil {
			return estimatedSize, ErrSinkTooSmall
		}

		err = chunk.Save(context.Background(), s)
		if err != nil {
			return estimatedSize, err
		}

		err = index.UpdateIndex(chunk.Header)
		if err != nil {
			return estimatedSize, err
		}
	}

	for _, key := range keys {
		s.Delete(context.Background(), key)
	}

	return estimatedSize, nil
}
