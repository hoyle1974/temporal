package chunks

import (
	"context"
	"time"

	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
)

type KVPair struct {
	Key  string
	Data []byte
}

// A sorted array of KVPairs
type KeyFrame []KVPair

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

// This is what is actually stored on disk
type ChunkData struct {
	Id        ChunkId
	Timestamp time.Time
	KeyFrame  KeyFrame
	Events    []Event
}

// This reprsents a chunk of data that would be stored on disk
type Chunk struct {
	Header Header
	Data   ChunkData
}

func (c *Chunk) ApplyEvent(e Event) {
	if c.Header.Max.Before(e.Timestamp) {
		c.Header.Max = e.Timestamp
	}
	c.Data.Events = append(c.Data.Events, e)
}

func (c *Chunk) SetKeyframe(state map[string][]byte) {
	c.Data.KeyFrame = make([]KVPair, 0, len(state))

	for k, v := range misc.Range(state) {
		c.Data.KeyFrame = append(c.Data.KeyFrame, KVPair{Key: k, Data: v})
	}
}

func NewChunk(timestamp time.Time) *Chunk {
	chunkId := NewChunkId(timestamp)

	header := Header{
		LastUpdate: time.Now().UTC(),
		Id:         chunkId,
		Min:        timestamp,
		Max:        timestamp,
	}

	data := ChunkData{
		Id:        chunkId,
		Timestamp: timestamp,
	}

	return &Chunk{
		Header: header,
		Data:   data,
	}
}

// Saves a header to the storage system
func (c Chunk) Save(ctx context.Context, s storage.System) error {
	c.Header.Save(ctx, s)

	chunkCache.Set(string(c.Header.Id), c, time.Minute)

	b, err := misc.EncodeToBytes(c.Data)
	if err != nil {
		return err
	}
	return s.Write(ctx, c.Data.Id.ChunkKey(), b)
}

func (c Chunk) GetStateAt(timestamp time.Time) (map[string][]byte, error) {
	ret := map[string][]byte{}
	for _, kv := range c.Data.KeyFrame {
		ret[kv.Key] = kv.Data
	}

	for _, event := range c.Data.Events {
		if event.Timestamp.After(timestamp) {
			return ret, nil
		}
		event.Apply(ret)
	}

	return ret, nil
}
