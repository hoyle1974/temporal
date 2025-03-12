package temporal

import (
	"fmt"
	"time"
	"weak"
)

type DiffAndCache struct {
	Diff Diff
	orig weak.Pointer[Keyframe]
}

type collated struct {
	Timestamp time.Time
	Keyframe  Keyframe
	Diffs     []DiffAndCache
}

func (c *collated) getFrame(timestamp time.Time) ([]byte, error) {
	inTime := timestamp.Truncate(time.Second)
	start := c.Timestamp

	if inTime.Equal(start) {
		return c.Keyframe, nil
	}

	var frame Keyframe
	frame = copyBytes(c.Keyframe)
	var err error
	for idx := 0; idx < len(c.Diffs); idx++ {
		start = start.Add(time.Second)
		if temp := c.Diffs[idx].orig.Value(); temp != nil {
			frame = *temp
		} else {
			frame, err = applyDiff(frame, c.Diffs[idx].Diff)
			if err != nil {
				return nil, err
			}
			var b Keyframe = copyBytes(frame)
			c.Diffs[idx].orig = weak.Make(&b)
		}

		if inTime.Equal(start) {
			return frame, nil
		}
	}

	return nil, fmt.Errorf("frame not found")
}
