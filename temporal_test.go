package temporal

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/google/uuid"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
)

func TestFoo(t *testing.T) {

}

func TestCollation(t *testing.T) {

	data := make([]Keyframe, 1000)
	for idx := 0; idx < len(data); idx++ {
		data[idx] = []byte(fmt.Sprintf("data [%d]", idx))
	}

	c := &collated{}
	c.Timestamp = time.Now().Truncate(time.Minute)
	pool := pond.NewPool(64)
	c = collateData(pool, data, c)
	a := time.Now()
	pool.StopAndWait()
	b := time.Since(a)
	fmt.Println(b)

	for idx := range len(data) {
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
	storage := storage.NewMemoryStorage()
	manager, err := NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	now := time.Now()

	reps := 2000
	for i := range reps {
		data := []byte(fmt.Sprintf("test data %d", i))
		err = manager.WriteData(context.Background(), now.Add(time.Duration(i)*time.Second), data)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	manager.Flush(context.Background())

	for i := range reps {
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
	storage := storage.NewMemoryStorage()
	manager, err := NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	now := time.Now()

	reps := 2000
	for i := range reps {
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

func TestPerformance(t *testing.T) {
	storage := storage.NewMemoryStorage()
	manager, err := NewStorageManager(storage)
	if err != nil {
		t.Fatalf("Failed to create StorageManager: %v", err)
	}

	now := time.Now()

	// Create a random set of of datas
	data := []string{}
	for t := range 1000 {
		data = append(data, fmt.Sprintf("Data line %d contains %s\n", t, uuid.NewString()))
	}

	reps := 2000
	for i := range reps {
		for k := 0; k < 5; k++ {
			idx := rand.Intn(len(data))
			data[idx] = fmt.Sprintf("Data line %d contains %s\n", idx, uuid.NewString())
		}
		b, err := misc.EncodeToBytes(data)
		if err != nil {
			t.Fatalf("error encoding: %v", err)
		}

		err = manager.WriteData(context.Background(), now.Add(time.Duration(i)*time.Second), b)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
	}

	fmt.Println(storage)
}

func TestRealData(t *testing.T) {
	storage := storage.NewDiskStorage("/Users/jstrohm/code/khronoscope/data")
	storageManager, err := NewStorageManager(storage, Config{
		MaxFileSize:   8 * 1024 * 1024,
		MaxDiffFrames: 599,
	})
	if err != nil {
		t.Fatalf("error creating new storage manager: %v", err)
	}
	// storageManager.PrintStats()

	min, _ := storageManager.GetMinMaxTime()

	begin := time.Now()
	b := time.Now()
	for idx := range 1000 {
		min = min.Add(time.Second)

		_, err := storageManager.ReadData(context.Background(), min)
		if err != nil {
			t.Fatalf("error reading data [%v] (%v) : %v", "", idx, err)
		}
		if (idx % 100) == 0 {
			diffTime := time.Since(b)
			fmt.Printf("%d) %v\n", idx, diffTime)
			b = time.Now()
		}
	}
	end := time.Since(begin)
	fmt.Printf("%v\n", end)

}

func newRandomData() []string {
	// Create a random set of of datas
	data := []string{}
	for t := range 1000 {
		data = append(data, fmt.Sprintf("Data line %d contains %s\n", t, uuid.NewString()))
	}
	return data
}

func mutateData(data []string) []string {
	for range 5 {
		idx := rand.Intn(len(data))
		data[idx] = fmt.Sprintf("Data line %d contains %s\n", idx, uuid.NewString())
	}
	return data
}

func BenchmarkReadData(b *testing.B) {
	// Are we profiling?
	if f, err := os.Create("temporal.pprof"); err != nil {
		b.Fatalf("error creating temporal.pprof file: %v", err)
	} else {
		if err := pprof.StartCPUProfile(f); err != nil {
			b.Fatalf("error starting CPU profile: %v", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Start writing data, when we know chunks exists
	// Start reading from the start, make sure we can do it in real time
	storage := storage.NewMemoryStorage()
	storageManager, err := NewStorageManager(storage, Config{
		MaxFileSize:   8 * 1024 * 1024,
		MaxDiffFrames: 599,
	})
	if err != nil {
		b.Fatalf("error creating new storage manager: %v", err)
	}

	now := time.Now()

	data := newRandomData()
	for idx := range 2000 {
		bytes, err := misc.EncodeToBytes(data)
		if err != nil {
			b.Fatalf("error encoding: %v", err)
		}
		storageManager.WriteData(context.Background(), now.Add(time.Duration(idx)*time.Second), bytes)
		data = mutateData(data)
	}
	storageManager.Flush(context.Background())

	storageManager, err = NewStorageManager(storage, Config{
		MaxFileSize:   8 * 1024 * 1024,
		MaxDiffFrames: 599,
	})
	if err != nil {
		b.Fatalf("error creating new storage manager: %v", err)
	}

	temp := 0
	for idx := range 2000 {
		data, err := storageManager.ReadData(context.Background(), now.Add(time.Duration(idx)*time.Second))
		if err != nil {
			b.Fatalf("error reading data(%d): %v", idx, err)
		}
		temp += len(data)
	}
	fmt.Println(temp)

}

// This is horrible code and the compiler should not allow it.
func bar() error {
	count := 0
	var value = "a string"
	var err error
	for range 1000 {
		value, err := foo()
		if err != nil {
			return err
		}
		count += value
	}
	fmt.Println(value, count)
	return err
}

func foo() (int, error) {
	return 0, nil
}
