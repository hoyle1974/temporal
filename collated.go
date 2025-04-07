package temporal

/*
func init() {
	gob.Register(diffFrame2{})
}

type DiffFrame interface {
	GetKeyframe(prev Keyframe) Keyframe
}

type diffFrameCache struct {
	cache weak.Pointer[Keyframe]
	orig  DiffFrame
}

func (d *diffFrameCache) GetKeyframe(prev Keyframe) Keyframe {
	if k := d.cache.Value(); k != nil {
		return *k
	}
	k := d.orig.GetKeyframe(prev)
	d.cache = weak.Make(&k)
	return k
}
func WrapWithCache(o DiffFrame, prev Keyframe) DiffFrame {
	d := &diffFrameCache{
		orig: o,
	}
	if prev != nil {
		go d.GetKeyframe(prev)
	}
	return d
}
func UnwrapCache(o DiffFrame) DiffFrame {
	if d, ok := o.(*diffFrameCache); ok {
		return d.orig
	}
	return o
}

type diffFrame1 struct {
	keyframe Keyframe
}

func (d diffFrame1) GetKeyframe(prev Keyframe) Keyframe {
	return d.keyframe
}

func (d diffFrame1) Convert(pool pond.Pool, index int, c *collated, _prev Keyframe) {
	var prev Keyframe = misc.CopyBytes(_prev)
	var current Keyframe = misc.CopyBytes(d.keyframe)

	pool.SubmitErr(func() error {
		d, err := generateDiff(prev, current)
		if err != nil {
			return err
		}
		c.Diffs[index] = WrapWithCache(diffFrame2{Diff: d}, prev)
		return nil
	})
}

type diffFrame2 struct {
	Diff Diff
}

func (d diffFrame2) GetKeyframe(prev Keyframe) Keyframe {
	k, _ := applyDiff(prev, d.Diff)
	return k
}

func NewDiffFrame1(current Keyframe) diffFrame1 {
	return diffFrame1{
		keyframe: misc.CopyBytes(current),
	}
}

type collated struct {
	Timestamp time.Time
	Keyframe  Keyframe
	Diffs     []DiffFrame
}

func (c *collated) PrintStats() {
	fmt.Printf("collated %v\n", c.Timestamp)

	size := len(c.Keyframe)
	size2 := size

	kf := c.Keyframe
	for _, d := range c.Diffs {
		size += len(d.(diffFrame2).Diff)
		kf = d.GetKeyframe(kf)
		size2 += len(kf)
	}

	r := size2 / size
	fmt.Printf("	Compressed: %v   Original: %v  Ration: 1 to %d\n", size, size2, r)
}

func (c *collated) getFrame(timestamp time.Time) ([]byte, error) {
	inTime := timestamp.Truncate(time.Second)
	start := c.Timestamp

	if inTime.Equal(start) {
		return c.Keyframe, nil
	}

	var frame Keyframe
	frame = misc.CopyBytes(c.Keyframe)
	for idx := 0; idx < len(c.Diffs); idx++ {
		start = start.Add(time.Second)
		frame = c.Diffs[idx].GetKeyframe(frame)

		if inTime.Equal(start) {
			return frame, nil
		}
	}

	return nil, fmt.Errorf("frame not found")
}

func collateData(pool pond.Pool, dataToCollate []Keyframe, c *collated) *collated {
	c.Keyframe = misc.CopyBytes(dataToCollate[0])
	c.Diffs = make([]DiffFrame, len(dataToCollate)-1)

	for idx := 1; idx < len(dataToCollate); idx++ {
		diff1 := NewDiffFrame1(dataToCollate[idx])
		c.Diffs[idx-1] = diff1
		diff1.Convert(pool, idx-1, c, dataToCollate[idx-1])
	}

	return c
}

*/
