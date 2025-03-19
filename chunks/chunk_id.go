package chunks

import "time"

// A chunk Id is in the form YYYY/MM/DD/YYYYMMDD_HHMMSS.sssssssss
// $ChunkId.data would be the key in a storage system to locate a chunks data
// $ChunkId.header would be the key in a storage system to locate a chunks meta data
type ChunkId string

const layout = "20060102_150405.000000000"

func NewChunkId(t time.Time) ChunkId {
	return ChunkId(t.UTC().Format(layout))
}
func (c ChunkId) HeaderKey() string { return string(c) + ".header" }
func (c ChunkId) ChunkKey() string  { return string(c) + ".chunk" }
