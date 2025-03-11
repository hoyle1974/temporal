package temporal

import (
	"bytes"
	"encoding/gob"
)

func encodeToBytes(obj any) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeToStruct(data []byte) (*collated, error) {
	var result collated
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func copyBytes(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}

func collateData(dataToCollate [][]byte, c collated) collated {
	c.Keyframe = copyBytes(dataToCollate[0])
	c.Diffs = make([]Diff, len(dataToCollate)-1)

	for idx := 1; idx < len(dataToCollate); idx++ {
		diff, err := generateDiff(dataToCollate[idx-1], dataToCollate[idx])
		if err != nil {
			return c
		}
		c.Diffs[idx-1] = diff
	}

	return c
}
