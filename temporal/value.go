package temporal

import (
	"sort"
	"time"
)

const KEYFRAME_RATE = 16

func NewTimeValueStore() *TimeValueStore {
	store := &TimeValueStore{
		Keyframes: []keyFrame{},
	}
	return store
}

// TimeValueStore holds the values and diffs with associated timestamps
type TimeValueStore struct {
	Keyframes []keyFrame
}

// keyFrame represents a snapshot of a value at a specific timestamp
// It also contains up a set number of DiffFrames that hold the diffs of the data
type keyFrame struct {
	Timestamp time.Time
	Value     []byte
	// DiffFrames []diffFrame
	// Last       []byte
}

/*
func (frame *keyFrame) FindNextTime(timestamp time.Time, dir int) (time.Time, error) {
	var times []time.Time

	if dir != 1 && dir != -1 {
		return time.Time{}, fmt.Errorf("invalid direction: %d", dir)
	}

	times = append(times, frame.Timestamp)
	for _, df := range frame.DiffFrames {
		times = append(times, df.Timestamp)
	}

	if dir == 1 {
		for _, t := range times {
			if t.After(timestamp) {
				return t, nil
			}
		}
	} else { // dir == -1
		var prevTime time.Time
		found := false
		for _, t := range times {
			if t.Before(timestamp) {
				prevTime = t
				found = true
			}
		}
		if found {
			return prevTime, nil
		}

	}

	return time.Time{}, fmt.Errorf("no time found in direction %d after %s", dir, timestamp)
}
*/

// func (frame *keyFrame) MinMax() (time.Time, time.Time) {
// 	if len(frame.DiffFrames) == 0 {
// 		return frame.Timestamp, frame.Timestamp
// 	}
// 	return frame.Timestamp, frame.DiffFrames[len(frame.DiffFrames)-1].Timestamp
// }

// func (frame *keyFrame) CheckForErrors() error {
// 	var r resources.Resource
// 	orig := frame.queryValue(frame.Timestamp.Time)
// 	err := misc.DecodeFromBytes(orig, &r)
// 	if err != nil {
// 		return err
// 	}

// 	for _, d := range frame.DiffFrames {
// 		b := frame.queryValue(d.Timestamp.Time)

// 		err := misc.DecodeFromBytes(b, &r)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// This keyFrame is expected to hold this value.  Query it and it's diffs to find the
// one that was valid for when this timestamp is
/*
func (frame *keyFrame) queryValue(timestamp time.Time) []byte {
	index := sort.Search(len(frame.DiffFrames), func(j int) bool {
		return frame.DiffFrames[j].Timestamp.After(timestamp)
	})

	// Handle the case where the timestamp is before all diffs
	if index == 0 && len(frame.DiffFrames) > 0 && timestamp.Before(frame.DiffFrames[0].Timestamp) {
		return frame.Value
	}

	// if index > 0 {
	return frame.DiffFrames[index-1].original
	// if ptr := frame.DiffFrames[index-1].original.Value(); ptr != nil {
	// 	return *ptr
	// } else {
	// }
	// }

	// cur := frame.Value
	// for i := 0; i < index; i++ {
	// 	cur, _ = applyDiff(cur, frame.DiffFrames[i].Diff)
	// }
	// return cur
}
*/

/*
func (frame *keyFrame) addDiffFrame(timestamp time.Time, value []byte) bool {
	if len(frame.DiffFrames) == KEYFRAME_RATE {
		if len(frame.DiffFrames) > 0 && frame.DiffFrames[len(frame.DiffFrames)-1].Timestamp.Before(timestamp) {
			return false // We have enough frames, and adding at the end
		}
	}

	if len(frame.DiffFrames) == 0 || (len(frame.DiffFrames) > 0 && frame.DiffFrames[len(frame.DiffFrames)-1].Timestamp.Before(timestamp)) {
		// diff, err := generateDiff(frame.Last, value) // Diff against full last value
		// if err != nil {
		// 	return false
		// }

		frame.DiffFrames = append(frame.DiffFrames, diffFrame{
			Timestamp: timestamp,
			// Diff:      diff,
			original: value, //weak.Make(&value),
		})

		frame.Last = value // Update stored full value for next append
		return true
	}

	// Decompress all frames
	curr := frame.Value
	times := make([]time.Time, len(frame.DiffFrames))
	original := make([][]byte, len(frame.DiffFrames))
	for idx, diffFrame := range frame.DiffFrames {
		actual, err := applyDiff(curr, diffFrame.Diff)
		if err != nil {
			return false // Stop if any diff application fails
		}
		original[idx] = actual
		times[idx] = diffFrame.Timestamp
		curr = actual
	}

	index := sort.Search(len(times), func(j int) bool {
		return times[j].After(timestamp)
	})

	original = append(original, nil)   // Extend the slice by one
	times = append(times, time.Time{}) // Extend the slice by one

	copy(original[index+1:], original[index:]) // Shift elements to the right
	copy(times[index+1:], times[index:])       // Shift elements to the right

	original[index] = value
	times[index] = timestamp

	frame.DiffFrames = []diffFrame{}

	// last := frame.Value
	for idx := 0; idx < len(original); idx++ {
		// diff, err := generateDiff(last, original[idx])
		// if err != nil {
		// 	panic(err)
		// }

		frame.DiffFrames = append(frame.DiffFrames, diffFrame{
			Timestamp: times[idx],
			// Diff:      diff,
			original: original[idx], //weak.Make(&original[idx]),
		})

		// last = original[idx]
	}

	return true
}
*/

// type diffFrame struct {
// 	Timestamp time.Time
// 	original  []byte
// 	// Diff      Diff
// 	// original  weak.Pointer[[]byte]
// }

// Add a value that is valid @timestamp and after
func (store *TimeValueStore) AddValue(timestamp time.Time, value []byte) {
	// Find the insertion point to maintain chronological order.
	index := sort.Search(len(store.Keyframes), func(j int) bool {
		return store.Keyframes[j].Timestamp.After(timestamp)
	})

	if len(store.Keyframes) == 0 {
		store.Keyframes = append(store.Keyframes, keyFrame{
			Timestamp: timestamp,
			Value:     value,
			// DiffFrames: make([]diffFrame, 0, KEYFRAME_RATE),
			// Last:       value,
		})
		return
	}

	// // index is where we would put this keyframe.  Backup one keyframe and see if we just want to append diff
	// if index > 0 && store.Keyframes[index-1].addDiffFrame(timestamp, value) { // Corrected: Check index > 0
	// 	return
	// }

	// Insert the new value at the correct position.
	store.Keyframes = append(store.Keyframes[:index], append([]keyFrame{{
		Timestamp: timestamp,
		Value:     value,
		// DiffFrames: make([]diffFrame, 0, KEYFRAME_RATE),
		// Last:       value,
	}}, store.Keyframes[index:]...)...)

}

func (store *TimeValueStore) QueryValue(timestamp time.Time) []byte {
	return store.queryValue(timestamp)
}

// Return the most recent value that was set on or before timestamp.
func (store *TimeValueStore) queryValue(timestamp time.Time) []byte {
	if len(store.Keyframes) == 0 { // No data
		return nil
	}

	index := sort.Search(len(store.Keyframes), func(j int) bool {
		return store.Keyframes[j].Timestamp.After(timestamp)
	})

	if index == 0 { // Timestamp is before the first keyframe
		return nil
	} else if index == len(store.Keyframes) { // Timestamp is after the last keyframe
		return store.Keyframes[index-1].Value //queryValue(timestamp) // Use the *last* keyframe
	} else { // Timestamp is within the keyframes
		return store.Keyframes[index-1].Value //queryValue(timestamp) // Use the keyframe *before*
	}
}

/*
func (store *TimeValueStore) FindNextTimeKey(timestamp time.Time, dir int) (time.Time, error) {
	if dir == 0 {
		return timestamp, fmt.Errorf("invalid direction: %d", dir)
	}

	// We are just going to brute force it
	times := []time.Time{}

	for _, k := range store.Keyframes {
		times = append(times, k.Timestamp)
		for _, d := range k.DiffFrames {
			times = append(times, d.Timestamp)
		}
	}

	if dir > 0 {
		for _, t := range times {
			if t.After(timestamp) {
				return t, nil
			}
		}
		return timestamp, errors.New("no next")
	}

	slices.Reverse(times)
	for _, t := range times {
		if t.Before(timestamp) {
			return t, nil
		}
	}
	return timestamp, errors.New("no next")
}
*/
