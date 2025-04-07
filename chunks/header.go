package chunks

import (
	"context"
	"time"

	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
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
		return h, errors.Wrap(err, "can not load header")
	} else if b == nil {
		return h, errors.New("header not found")
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

func (h Header) RemoveFromStorage(ctx context.Context, s storage.System) error {
	err := s.Delete(ctx, h.Id.HeaderKey())
	if err != nil {
		return errors.Wrap(err, "can not delete header")
	}
	err = s.Delete(ctx, h.Id.ChunkKey())
	if err != nil {
		return errors.Wrap(err, "can not delete chunk")
	}
	return nil
}

// Saves a header to the storage system
func (h Header) Save(ctx context.Context, s storage.System) error {
	if h.LastUpdate.IsZero() {
		panic("h.LastUpdate was zero")
	}
	if h.Min.IsZero() {
		panic("h.LastUpdate was zero")
	}
	if h.Max.IsZero() {
		panic("h.LastUpdate was zero")
	}
	b, err := misc.EncodeToBytes(h)
	if err != nil {
		return errors.Wrap(err, "can not encode header to bytes to save")
	}
	err = s.Write(ctx, h.Id.HeaderKey(), b)
	if err != nil {
		return errors.Wrap(err, "can not save header")
	}
	return nil
}

// Loads the chunk associated with this header
func (h Header) LoadChunk(ctx context.Context, s storage.System) (Chunk, error) {
	if chunk, ok := chunkCache.Get(string(h.Id)); ok {
		chunkCacheStats.Hit()
		return chunk.(Chunk), nil
	} else {
		chunkCacheStats.Miss()
	}

	var cd ChunkData
	b, err := s.Read(ctx, h.Id.ChunkKey())
	if err != nil {
		chunkCache.Set(string(h.Id), Chunk{}, cache.DefaultExpiration)
		return Chunk{}, err
	}
	cd.diskSize = len(b)

	err = misc.DecodeFromBytes(b, &cd) // This might come to bite me in the future
	if err != nil {
		chunkCache.Set(string(h.Id), Chunk{}, cache.DefaultExpiration)
		return Chunk{}, errors.Wrap(err, "can not decode chunk")
	}

	cd.populateNonSerializedData()

	chunk := Chunk{Header: h, Data: cd}
	chunkCache.Set(string(h.Id), chunk, cache.DefaultExpiration)

	return chunk, nil
}
