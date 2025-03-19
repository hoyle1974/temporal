package chunks

import (
	"context"
	"time"

	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
)

// This reprsents a chunk of data that would be stored on disk
// It is small so it can quickly be overwritten.
type Header struct {
	LastUpdate time.Time
	Id         ChunkId
	Prev       ChunkId
	Next       ChunkId
	Min        time.Time
	Max        time.Time
}

// Loads a header from the storage system
func LoadHeader(ctx context.Context, s storage.System, id ChunkId) (Header, error) {
	var h Header
	if b, err := s.Read(ctx, id.HeaderKey()); err != nil {
		return h, err
	} else {
		return h, misc.DecodeFromBytes(b, &h) // This might come to bite me in the future
	}
}

func (h Header) ResponsibleFor(timestamp time.Time) bool {
	if (timestamp.Equal(h.Min) || timestamp.After(h.Min)) && (timestamp.Equal(h.Min) || timestamp.Before(h.Max)) {
		return true
	}
	return false
}

// Saves a header to the storage system
func (h Header) Save(ctx context.Context, s storage.System) error {
	b, err := misc.EncodeToBytes(h)
	if err != nil {
		return err
	}
	return s.Write(ctx, h.Id.HeaderKey(), b)
}

// Loads the chunk associated with this header
func (h Header) LoadChunk(ctx context.Context, s storage.System) (Chunk, error) {
	var cd ChunkData
	b, err := s.Read(ctx, h.Id.ChunkKey())
	if err != nil {
		return Chunk{}, err
	}

	err = misc.DecodeFromBytes(b, &cd) // This might come to bite me in the future
	if err != nil {
		return Chunk{}, err
	}

	return Chunk{Header: h, Data: cd}, nil
}
