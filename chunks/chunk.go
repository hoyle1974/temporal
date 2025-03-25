package chunks

import (
	"context"
	"fmt"
	"sort"
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

type IndexedKVPair struct {
	KeyIndex int32
	Data     []byte
}

type IndexedKeyFrame []IndexedKVPair

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

type DiffEvent struct {
	Timestamp time.Time
	KeyIndex  int32
	Diff      Diff
}

// This is what is actually stored on disk
type ChunkData struct {
	Id              ChunkId
	Timestamp       time.Time
	Keys            []string
	IndexedKeyFrame IndexedKeyFrame
	Diffs           []DiffEvent

	keyToIndex map[string]int32
	indexToKey map[int32]string
	frames     [][]byte
}

func (cd *ChunkData) populateNonSerializedData() {
	cd.keyToIndex = map[string]int32{}
	cd.indexToKey = map[int32]string{}
	for idx, k := range cd.Keys {
		cd.keyToIndex[k] = int32(idx)
		cd.indexToKey[int32(idx)] = k
	}
	cd.frames = make([][]byte, len(cd.Diffs))
}

// This reprsents a chunk of data that would be stored on disk
type Chunk struct {
	Header Header
	Data   ChunkData
}

func (c *Chunk) Finish(keyFrame KeyFrame, events []Event) {
	// Find and index all keys
	keyMap := map[string]int32{}
	for idx, kv := range keyFrame {
		keyMap[kv.Key] = int32(idx)
	}
	for _, e := range events {
		if _, ok := keyMap[e.Key]; !ok {
			keyMap[e.Key] = int32(len(keyMap))
		}
	}
	c.Data.Keys = make([]string, len(keyMap))
	for k, v := range keyMap {
		c.Data.Keys[v] = k
	}
	c.Data.IndexedKeyFrame = NewIndexedKeyFrame(keyFrame, keyMap)

	// Sort the events by key
	eventsByKey := make(map[string][]Event)
	for _, e := range events {
		if c.Header.Max.Before(e.Timestamp) {
			c.Header.Max = e.Timestamp
		}
		eventsByKey[e.Key] = append(eventsByKey[e.Key], e)
	}

	// Create a channel to receive diffs
	diffChan := make(chan []DiffEvent)
	defer close(diffChan)

	state := map[string][]byte{}
	for _, kv := range keyFrame {
		state[kv.Key] = kv.Data
	}

	// Process each key's events concurrently
	for key, events := range eventsByKey {
		go func(key string, value []byte, events []Event) {
			diffs := []DiffEvent{}
			for _, e := range events {
				if e.Delete {
					diffs = append(diffs, DiffEvent{Timestamp: e.Timestamp, KeyIndex: keyMap[e.Key], Diff: Diff{}})
					value = []byte{}
				} else {
					diff, err := generateDiff(value, e.Data)
					if err != nil {
						fmt.Println(err)
						panic(err)
					}
					diffs = append(diffs, DiffEvent{Timestamp: e.Timestamp, KeyIndex: keyMap[e.Key], Diff: diff})
					value = e.Data
				}
			}
			// Send the diffs to a channel
			diffChan <- diffs
		}(key, state[key], events)
	}

	for range len(eventsByKey) {
		diffs := <-diffChan
		c.Data.Diffs = append(c.Data.Diffs, diffs...)
	}

	// Sort c.Data.Diffs by it's timestamp
	sort.Slice(c.Data.Diffs, func(i, j int) bool {
		return c.Data.Diffs[i].Timestamp.Before(c.Data.Diffs[j].Timestamp)
	})

	c.Data.populateNonSerializedData()
}

func NewKeyFrame(state map[string][]byte) KeyFrame {
	keyFrame := make([]KVPair, 0, len(state))

	for k, v := range misc.Range(state) {
		keyFrame = append(keyFrame, KVPair{Key: k, Data: v})
	}

	return keyFrame
}

func NewIndexedKeyFrame(keyFrame KeyFrame, keyIndex map[string]int32) IndexedKeyFrame {
	indexedKeyFrame := make([]IndexedKVPair, 0, len(keyFrame))

	for _, kv := range keyFrame {
		indexedKeyFrame = append(indexedKeyFrame, IndexedKVPair{KeyIndex: keyIndex[kv.Key], Data: kv.Data})
	}

	return indexedKeyFrame
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
	for _, kv := range c.Data.IndexedKeyFrame {
		ret[c.Data.indexToKey[kv.KeyIndex]] = kv.Data
	}

	for idx, diff := range c.Data.Diffs {
		if diff.Timestamp.After(timestamp) {
			return ret, nil
		}
		if c.Data.frames[idx] == nil {
			n, _ := applyDiff(ret[c.Data.indexToKey[diff.KeyIndex]], diff.Diff)
			ret[c.Data.indexToKey[diff.KeyIndex]] = n
			c.Data.frames[idx] = n
		} else {
			ret[c.Data.indexToKey[diff.KeyIndex]] = c.Data.frames[idx]
		}
	}

	return ret, nil
}
