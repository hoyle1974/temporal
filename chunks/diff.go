package chunks

import (
	"fmt"

	"github.com/gabstv/go-bsdiff/pkg/bsdiff"
	"github.com/gabstv/go-bsdiff/pkg/bspatch"
)

type Diff []byte

func generateDiff(a, b []byte) (Diff, error) {
	patch, err := bsdiff.Bytes(a, b)
	if err != nil {
		return Diff{}, err
	}

	if len(patch) >= len(b) {
		// Store raw data with a "0" prefix
		return append([]byte{0}, b...), nil
	}

	// Store diff with a "1" prefix
	return append([]byte{1}, patch...), nil
}

func applyDiff(a []byte, diffData Diff) ([]byte, error) {
	if len(diffData) == 0 {
		return a, nil
	}

	// Check prefix
	switch diffData[0] {
	case 0:
		// Raw data case
		return diffData[1:], nil
	case 1:
		// Diff case
		newfile, err := bspatch.Bytes(a, diffData[1:])
		if err != nil {
			return []byte{}, err
		}
		return newfile, nil
	default:
		return []byte{}, fmt.Errorf("invalid diff format")
	}
}
