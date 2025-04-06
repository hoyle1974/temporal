package temporal

import (
	"bytes"
	"testing"
	"time"
)

func Test_NoValues(t *testing.T) {
	store := NewTimeValueStore()

	if store.QueryValue(time.Now()) != nil {
		t.Fatalf("Expected nil value")
	}

}

func Test_OneValue(t *testing.T) {
	store := NewTimeValueStore()

	t1 := time.Now()
	store.AddValue(t1.Add(time.Second), []byte{1, 2, 3})

	if store.QueryValue(t1) != nil {
		t.Fatalf("Expected nil value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*2)), []byte{1, 2, 3}) {
		t.Fatalf("Expected a value")
	}
}

func Test_TwoValue(t *testing.T) {
	store := NewTimeValueStore()

	t1 := time.Now()
	store.AddValue(t1.Add(time.Second), []byte{1, 2, 3})
	store.AddValue(t1.Add(time.Second*5), []byte{2, 3, 4})

	if store.QueryValue(t1) != nil {
		t.Fatalf("Expected nil value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*2)), []byte{1, 2, 3}) {
		t.Fatalf("Expected a value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*6)), []byte{2, 3, 4}) {
		t.Fatalf("Expected a value")
	}
}

func Test_ThreeValue(t *testing.T) {
	store := NewTimeValueStore()

	t1 := time.Now()
	store.AddValue(t1.Add(time.Second), []byte{1, 2, 3})
	store.AddValue(t1.Add(time.Second*5), []byte{2, 3, 4})
	store.AddValue(t1.Add(time.Second*10), []byte{3, 4, 5})

	if store.QueryValue(t1) != nil {
		t.Fatalf("Expected nil value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*2)), []byte{1, 2, 3}) {
		t.Fatalf("Expected a value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*6)), []byte{2, 3, 4}) {
		t.Fatalf("Expected a value")
	}
	if !bytes.Equal(store.QueryValue(t1.Add(time.Second*11)), []byte{3, 4, 5}) {
		t.Fatalf("Expected a value")
	}
}

func Test_ManyValues(t *testing.T) {
	store := NewTimeValueStore()

	t1 := time.Now()
	for i := 0; i < 1000; i++ {
		store.AddValue(t1.Add(time.Second+(time.Duration(i)*time.Second)), []byte{byte(i)})
	}

	if store.QueryValue(t1) != nil {
		t.Fatalf("Expected nil value")
	}

	for i := 0; i < 100; i++ {
		offset := time.Second + (time.Duration(i) * time.Second) + time.Millisecond
		if !bytes.Equal(store.QueryValue(t1.Add(offset)), []byte{byte(i)}) {
			t.Fatalf("Expected a value")
		}
	}
}
