package main

import (
	"fmt"
	"time"
)

type collated struct {
	Timestamp time.Time
	Keyframe  Keyframe
	Diffs     []Diff
}

func (c *collated) getFrame(timestamp time.Time) ([]byte, error) {
	inTime := timestamp.Truncate(time.Second)
	start := c.Timestamp

	if inTime.Equal(start) {
		return c.Keyframe, nil
	}

	frame := copyBytes(c.Keyframe)
	var err error
	for idx := 0; idx < len(c.Diffs); idx++ {
		start = start.Add(time.Second)
		frame, err = applyDiff(frame, c.Diffs[idx])
		if err != nil {
			return nil, err
		}
		if inTime.Equal(start) {
			return frame, nil
		}
	}

	return nil, fmt.Errorf("frame not found")
}
