package temporal

import (
	"time"
)

type TimeRange struct {
	Min time.Time
	Max time.Time
}

func (t *TimeRange) Adjust(timestamp time.Time) {
	if t.Min.IsZero() || timestamp.Before(t.Min) {
		t.Min = timestamp
	}
	if t.Max.IsZero() || timestamp.After(t.Max) {
		t.Max = timestamp
	}
}

func (t *TimeRange) Contains(timestamp time.Time) bool {
	return (timestamp.Equal(t.Min) || timestamp.After(t.Min)) &&
		(timestamp.Equal(t.Max) || timestamp.Before(t.Max))
}
