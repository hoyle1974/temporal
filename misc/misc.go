package misc

import (
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"
)

// EncodeToBytes compresses and serializes the data
func EncodeToBytes(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := gob.NewEncoder(gz)

	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}

	err = gz.Close() // Ensure all data is flushed
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecodeFromBytes decompresses and deserializes the data
func DecodeFromBytes(data []byte, a any) error {
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return err
	}
	defer gz.Close()

	dec := gob.NewDecoder(gz)
	return dec.Decode(a)
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

type NoCopy struct{}

func (*NoCopy) Lock()   {}
func (*NoCopy) Unlock() {}
