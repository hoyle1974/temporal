package temporal

import (
	"github.com/gabstv/go-bsdiff/pkg/bsdiff"
	"github.com/gabstv/go-bsdiff/pkg/bspatch"
)

type Keyframe []byte

type Diff []byte

func generateDiff(a, b Keyframe) (Diff, error) {
	patch, err := bsdiff.Bytes(a, b)
	if err != nil {
		return Diff{}, err
	}

	return patch, nil
}

func applyDiff(a Keyframe, diffData Diff) (Keyframe, error) {
	if len(diffData) == 0 {
		return a, nil
	}
	newfile, err := bspatch.Bytes(a, diffData)
	if err != nil {
		return []byte{}, err
	}

	return newfile, nil
}
