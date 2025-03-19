package misc

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

/*
func EncodeToBytes(obj any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeToStruct(data []byte) (*collated, error) {
	var result collated
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
*/

func EncodeToBytes(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeFromBytes(data []byte, a any) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(a)
	return err
}

func CopyBytes(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}

func DeepCopyArray[K any](s []K) []K {
	dest := make([]K, len(s))

	for k := 0; k < len(s); k++ {
		dest[k] = deepCopyValue(s[k])
	}

	return dest
}

func DeepCopyMap[K comparable, V any](m map[K]V) map[K]V {
	newMap := make(map[K]V, len(m))

	for k, v := range m {
		newMap[k] = deepCopyValue(v)
	}

	return newMap
}

func deepCopyValue[V any](v V) V {
	switch v := any(v).(type) {
	case map[any]any:
		return any(DeepCopyMap(v)).(V)
	case []any:
		return any(deepCopySlice(v)).(V)
	default:
		return v.(V)
	}
}

func deepCopySlice[V any](s []V) []V {
	newSlice := make([]V, len(s))
	for i, v := range s {
		newSlice[i] = deepCopyValue(v)
	}
	return newSlice
}

func minMaxTime(min time.Time, max time.Time, newTime time.Time) (time.Time, time.Time) {
	if !newTime.IsZero() {
		if newTime.Before(min) {
			min = newTime
		}
		if newTime.After(max) {
			max = newTime
		}
	}

	return min, max

}

func RandomTimeBetween(start, end time.Time) (time.Time, error) {
	if end.Before(start) {
		return time.Time{}, fmt.Errorf("end time must be after start time")
	}

	// Calculate the duration between the two times.
	duration := end.Sub(start)

	// Generate a random duration within the total duration.
	randomDuration := time.Duration(rand.Int63n(duration.Nanoseconds()))

	// Add the random duration to the start time.
	randomTime := start.Add(randomDuration)

	return randomTime, nil
}
