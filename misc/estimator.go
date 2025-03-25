package misc

type EventSizeEstimator struct {
	totalEvents int
	totalSize   int64
	alpha       float64
}

func NewEstimator(alpha float64) *EventSizeEstimator {
	return &EventSizeEstimator{alpha: alpha}
}

// Update updates the estimator with a new observation, using exponential moving average for adaptability.
func (e *EventSizeEstimator) Update(size int64, events int) {
	if e.totalEvents == 0 || e.totalSize == 0 {
		e.totalSize = size
		e.totalEvents = events
		return
	}

	// Apply exponential moving average for adaptive learning
	e.totalSize = int64(e.alpha*float64(size) + (1-e.alpha)*float64(e.totalSize))
	e.totalEvents = int(e.alpha*float64(events) + (1-e.alpha)*float64(e.totalEvents))
}

// Estimate calculates how many events are needed to reach the target size.
func (e *EventSizeEstimator) Estimate(targetSize int64) int {
	if e.totalEvents == 0 || e.totalSize == 0 {
		return 0 // Not enough data to estimate
	}
	avgSizePerEvent := float64(e.totalSize) / float64(e.totalEvents)
	return int(float64(targetSize) / avgSizePerEvent)
}
