package temporal

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hoyle1974/temporal/chunks"
	"github.com/hoyle1974/temporal/misc"
	"github.com/hoyle1974/temporal/storage"
)

func TestMap1(t *testing.T) {
	storage := storage.NewMemoryStorage()
	m, err := NewMap(storage)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	err = m.Set(context.Background(), time.Now(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	temp, err := m.Get(context.Background(), time.Now(), "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "bar" {
		t.Fatalf("wrong value: %s", string(temp))
	}
}

func TestMap2(t *testing.T) {
	s := storage.NewMemoryStorage()
	m, err := NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	err = m.Set(context.Background(), time.Now(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}

	m, err = NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	temp, err := m.Get(context.Background(), time.Now(), "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "bar" {
		t.Fatalf("wrong value: %s", string(temp))
	}
}

func TestMap3(t *testing.T) {
	s := storage.NewMemoryStorage()
	m, err := NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	a := time.Now()
	err = m.Set(context.Background(), a, "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("map set failed for foo: %v", err)
	}
	b := time.Now()
	err = m.Set(context.Background(), b, "bar", []byte("foo"))
	if err != nil {
		t.Fatalf("map set failed for bar: %v", err)
	}

	m, err = NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	temp, err := m.Get(context.Background(), time.Now(), "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "bar" {
		t.Fatalf("wrong value for foo: %s", string(temp))
	}
	temp, err = m.Get(context.Background(), time.Now(), "bar")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "foo" {
		t.Fatalf("wrong value for bar: %s", string(temp))
	}
}

func TestMap4(t *testing.T) {
	s := storage.NewMemoryStorage()
	m, err := NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	a := time.Now()
	err = m.Set(context.Background(), a, "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("map set failed for foo: %v", err)
	}
	b := time.Now()
	err = m.Set(context.Background(), b, "bar", []byte("foo"))
	if err != nil {
		t.Fatalf("map set failed for bar: %v", err)
	}
	c := time.Now()
	err = m.Set(context.Background(), c, "foo", []byte("foobar"))
	if err != nil {
		t.Fatalf("map set failed for foo: %v", err)
	}

	m, err = NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	temp, err := m.Get(context.Background(), time.Now(), "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "foobar" {
		t.Fatalf("wrong value for foo: %s", string(temp))
	}
	temp, err = m.Get(context.Background(), b, "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "bar" {
		t.Fatalf("wrong value for foo: %s", string(temp))
	}
	temp, err = m.Get(context.Background(), time.Now(), "bar")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "foo" {
		t.Fatalf("wrong value for bar: %s", string(temp))
	}

	state, err := m.GetAll(context.Background(), a.Add(-time.Second))
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if len(state) != 0 {
		t.Fatalf("state has values before they were written")
	}

	state, err = m.GetAll(context.Background(), c)
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if len(state) != 2 {
		t.Fatalf("state has wrong number of values before they were written")
	}
	if string(state["foo"]) != "foobar" {
		t.Fatalf("wrong value for foo: %s", state["foo"])
	}

	state, err = m.GetAll(context.Background(), b)
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if len(state) != 2 {
		t.Fatalf("state has wrong number of values before they were written")
	}
	if string(state["foo"]) != "bar" {
		t.Fatalf("wrong value for foo: %s", state["foo"])
	}
}

func TestMap5(t *testing.T) {
	s := storage.NewMemoryStorage()
	m, err := NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	err = m.Set(context.Background(), time.Now(), "foo", []byte("bar"))
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}

	m, err = NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	m, err = NewMap(s)
	if err != nil {
		t.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		t.Fatalf("map was nil")
	}

	temp, err := m.Get(context.Background(), time.Now(), "foo")
	if err != nil {
		t.Fatalf("map set failed: %v", err)
	}
	if string(temp) != "bar" {
		t.Fatalf("wrong value: %s", string(temp))
	}
}

func Benchmark1(b *testing.B) {
	s := storage.NewMemoryStorage()
	// s := storage.NewDiskStorage("data")
	m, err := NewMap(s)
	if err != nil {
		b.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		b.Fatalf("map was nil")
	}

	start := time.Now()
	items := 10000
	for idx := range items {
		key := fmt.Sprintf("key%d", idx)
		m.Set(context.Background(), start, key, []byte(uuid.NewString()))
	}

	for _ = range 100000 {
		key := fmt.Sprintf("key%d", rand.Int()%items)
		m.Set(context.Background(), time.Now(), key, []byte(uuid.NewString()))

		if rand.Int()%1000 == 0 {
			m, err = NewMap(s)
			if err != nil {
				b.Fatalf("could not create map: %v", err)
			}
			if m == nil {
				b.Fatalf("map was nil")
			}
		}
	}
	end := time.Now()

	m, err = NewMap(s)
	if err != nil {
		b.Fatalf("could not create map: %v", err)
	}
	if m == nil {
		b.Fatalf("map was nil")
	}

	chunks.ClearCache()
	b.ResetTimer()

	fmt.Println(b.N)
	for i := 0; i < b.N; i++ {
		r, _ := misc.RandomTimeBetween(start, end)
		state, err := m.GetAll(context.Background(), r)
		if err != nil {
			b.Fatalf("Error getting all: %v (%v %v %v)", err, r, start, end)
		}
		if len(state) == 0 {
			b.Fatalf("state was empty")
		}
	}

	chunks.PrintCacheStats()

}
