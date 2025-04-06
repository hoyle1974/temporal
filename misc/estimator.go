package misc

import (
	"sync"
)

type CompressionEstimator struct {
	_                NoCopy
	lock             sync.Mutex
	flushThreshold   int64
	currentSize      int64
	compressionRatio float64
}

func NewCompressionEstimator(flushThreshold int64) *CompressionEstimator {
	return &CompressionEstimator{
		flushThreshold:   flushThreshold,
		compressionRatio: 1.0,
	}
}

func (e *CompressionEstimator) OnWriteData(bytesWritten int64) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.currentSize += bytesWritten
}

func (e *CompressionEstimator) ShouldTryFlush() bool {
	e.lock.Lock()
	defer e.lock.Unlock()
	return float64(e.currentSize)*e.compressionRatio >= float64(e.flushThreshold)
}

func (e *CompressionEstimator) OnFlush(compressedSize int64, success bool) {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.compressionRatio = float64(compressedSize) / float64(e.currentSize)

	if success {
		e.currentSize = 0
	}
}
