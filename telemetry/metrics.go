package telemetry

// This package is how we write metrics in Temporal.  By default they are no-ops.
// But a user can provide an implementation if they want their metrics to go somewhere.

type Metrics interface {
	SetCount(key string, value int64)
	SetGuage(key string, value float64)
}

type NOPMetrics struct {
}

func (n NOPMetrics) SetCount(key string, value int64) {
}
func (n NOPMetrics) SetGuage(key string, value float64) {
}
