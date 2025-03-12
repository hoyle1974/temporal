package temporal

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCollation(t *testing.T) {

	data := make([]Keyframe, 60)
	for idx := 0; idx < 60; idx++ {
		data[idx] = []byte(fmt.Sprintf("data [%d]", idx))
	}

	var c collated
	c.Timestamp = time.Now().Truncate(time.Minute)
	c = collateData(data, c)

	for idx := 0; idx < 60; idx++ {
		a := string(data[idx])
		b, err := c.getFrame(c.Timestamp.Add(time.Second * time.Duration(idx)))
		if err != nil {
			t.Fatalf("Can't get frame %d", idx)
		}
		if string(b) != a {
			t.Fatalf("Frame didn't match, got '%s' but expected '%s' for frame %d", string(b), a, idx)
		}
	}

}

func TestWriteData(t *testing.T) {
	storage := NewMemoryStorage()
	manager, err := NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	now := time.Now()

	reps := 2000
	for i := 0; i < reps; i++ {
		data := []byte(fmt.Sprintf("test data %d", i))
		err = manager.WriteData(context.Background(), now.Add(time.Duration(i)*time.Second), data)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	for i := 0; i < reps; i++ {
		data := []byte(fmt.Sprintf("test data %d", i))
		temp, err := manager.ReadData(context.Background(), now.Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("Failed to read data (i=%d): %v", i, err)
		}

		if string(data) != string(temp) {
			t.Fatalf("Retrieved data does not match written data. Expected: '%s', Got: '%s' for idx=%d", string(data), string(temp), i)
		}
	}

}

func TestMinMaxTime(t *testing.T) {
	storage := NewMemoryStorage()
	manager, err := NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	now := time.Now()

	reps := 2000
	for i := 0; i < reps; i++ {
		data := []byte(fmt.Sprintf("test data %d", i))
		err = manager.WriteData(context.Background(), now.Add(time.Duration(i)*time.Second), data)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	min1, _ := manager.GetMinMaxTime()

	manager, err = NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	min2, _ := manager.GetMinMaxTime()

	if !min1.Equal(min2) {
		t.Fatalf("min1(%v) was not equal to min2(%v)", min1, min2)
	}

}
