package misc

import (
	"fmt"
	"reflect"
	"sort"
)

func Range[K comparable, V any](m map[K]V) func(func(K, V) bool) {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	// Sort the keys dynamically based on their type.
	sort.Slice(keys, func(i, j int) bool {
		return compareKeys(keys[i], keys[j]) < 0
	})

	// Return a function that takes a callback to process each key-value pair.
	return func(callback func(K, V) bool) {
		for _, key := range keys {
			value := m[key]
			// If the callback returns false, stop iteration.
			if !callback(key, value) {
				break
			}
		}
	}
}

// compareKeys determines how to sort keys dynamically.
// compareKeys determines how to sort keys dynamically.
func compareKeys[K comparable](a, b K) int {
	// Use reflection to determine the type of the key.
	kindA := reflect.TypeOf(a).Kind()
	kindB := reflect.TypeOf(b).Kind()

	// It's good practice to ensure both keys are of the same kind for comparison
	if kindA != kindB {
		// You might want to define a specific logic for comparing different kinds
		// For now, let's fall back to string comparison as before
		strA := fmt.Sprintf("%v", a)
		strB := fmt.Sprintf("%v", b)
		if strA < strB {
			return -1
		} else if strA > strB {
			return 1
		}
		return 0
	}

	switch kindA {
	case reflect.String:
		// Compare strings lexicographically.
		strA := fmt.Sprintf("%v", a)
		strB := fmt.Sprintf("%v", b)
		if strA < strB {
			return -1
		} else if strA > strB {
			return 1
		}
		return 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Compare signed integers numerically.
		valA := reflect.ValueOf(a).Int()
		valB := reflect.ValueOf(b).Int()
		if valA < valB {
			return -1
		} else if valA > valB {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Compare unsigned integers numerically.
		valA := reflect.ValueOf(a).Uint()
		valB := reflect.ValueOf(b).Uint()
		if valA < valB {
			return -1
		} else if valA > valB {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		// Compare floating-point numbers numerically.
		valA := reflect.ValueOf(a).Float()
		valB := reflect.ValueOf(b).Float()
		if valA < valB {
			return -1
		} else if valA > valB {
			return 1
		}
		return 0
	default:
		// Convert other types to strings and compare lexicographically.
		strA, strB := fmt.Sprintf("%v", a), fmt.Sprintf("%v", b)
		if strA < strB {
			return -1
		} else if strA > strB {
			return 1
		}
		return 0
	}
}
