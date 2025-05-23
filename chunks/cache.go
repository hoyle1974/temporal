package chunks

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hoyle1974/temporal/misc"
	"github.com/patrickmn/go-cache"
)

var chunkCache = cache.New(time.Minute, time.Minute)

type CacheStats struct {
	_      misc.NoCopy
	Hits   atomic.Int64
	Misses atomic.Int64
}

func (c *CacheStats) Hit() {
	c.Hits.Add(1)
}
func (c *CacheStats) Miss() {
	c.Misses.Add(1)
}
func (c *CacheStats) Reset() {
	c.Hits.Store(0)
	c.Misses.Store(0)
}
func (c *CacheStats) String() string {
	return fmt.Sprintf("CacheStats(Hits: %d, Misses: %d)", c.Hits.Load(), c.Misses.Load())
}

var chunkCacheStats = CacheStats{}

func PrintCacheStats() {
	fmt.Println(chunkCacheStats.String())
}

func ClearCache() {
	chunkCacheStats.Reset()
	chunkCache = cache.New(time.Minute, time.Minute)
}
