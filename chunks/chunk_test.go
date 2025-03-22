package chunks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hoyle1974/temporal/storage"
	"github.com/patrickmn/go-cache"
)

func TestEmptyChunk(t *testing.T) {
	chunk := NewChunk(time.Now())
	if chunk == nil {
		t.Fatalf("could not create new chunk")
	}

	state := map[string][]byte{}

	keyFrame := NewKeyFrame(state)

	events := []Event{}

	chunk.Finish(keyFrame, events)
}

func TestKeyFrameOnly(t *testing.T) {
	chunk := NewChunk(time.Now())
	if chunk == nil {
		t.Fatalf("could not create new chunk")
	}

	state := map[string][]byte{}
	state["foo"] = []byte("bar")
	state["bar"] = []byte("foo")

	keyFrame := NewKeyFrame(state)

	events := []Event{}

	chunk.Finish(keyFrame, events)

	stateAt, err := chunk.GetStateAt(time.Now())
	if err != nil {
		t.Fatalf("GetStateAt failed 1")
	}
	if string(stateAt["foo"]) != "bar" {
		t.Fatalf("foo should be bar 1")
	}
	if string(stateAt["bar"]) != "foo" {
		t.Fatalf("bar should be foo 1")
	}

	mem := storage.NewMemoryStorage()
	chunk.Save(context.Background(), mem)

	// Reset cache
	chunkCache = cache.New(time.Minute, time.Minute)

	chunk2, err := chunk.Header.LoadChunk(context.Background(), mem)
	if err != nil {
		t.Fatalf("could not load chunk: %v", err)
	}

	stateAt, err = chunk2.GetStateAt(time.Now())
	if err != nil {
		t.Fatalf("GetStateAt failed 2")
	}
	if string(stateAt["foo"]) != "bar" {
		t.Fatalf("foo should be bar 2")
	}
	if string(stateAt["bar"]) != "foo" {
		t.Fatalf("bar should be foo 2")
	}

}

func TestEventsOnly(t *testing.T) {
	chunk := NewChunk(time.Now())
	if chunk == nil {
		t.Fatalf("could not create new chunk")
	}

	state := map[string][]byte{}
	keyFrame := NewKeyFrame(state)

	events := []Event{
		{time.Now(), "foo", []byte("bar"), false},
		{time.Now(), "bar", []byte("foo"), false},
	}

	chunk.Finish(keyFrame, events)

	mem := storage.NewMemoryStorage()
	chunk.Save(context.Background(), mem)

	// Reset cache
	chunkCache = cache.New(time.Minute, time.Minute)

	chunk2, err := chunk.Header.LoadChunk(context.Background(), mem)
	if err != nil {
		t.Fatalf("could not load chunk: %v", err)
	}
	fmt.Println(chunk2)
}

func TestKeyFrameAndEvents(t *testing.T) {
	chunk := NewChunk(time.Now())
	if chunk == nil {
		t.Fatalf("could not create new chunk")
	}

	state := map[string][]byte{}
	state["foo"] = []byte("bar")
	state["bar"] = []byte("foo")
	keyFrame := NewKeyFrame(state)

	events := []Event{
		{time.Now().Add(time.Nanosecond), "foo", []byte("bar1"), false},
		{time.Now().Add(time.Nanosecond * 1), "bar", []byte("foo1"), false},
		{time.Now().Add(time.Nanosecond * 2), "baz", []byte("foobar"), false},
		{time.Now().Add(time.Nanosecond * 3), "foo", []byte{}, true},
		{time.Now().Add(time.Nanosecond * 4), "bar", []byte{}, true},
	}

	chunk.Finish(keyFrame, events)

	mem := storage.NewMemoryStorage()
	chunk.Save(context.Background(), mem)

	// Reset cache
	chunkCache = cache.New(time.Minute, time.Minute)

	chunk2, err := chunk.Header.LoadChunk(context.Background(), mem)
	if err != nil {
		t.Fatalf("could not load chunk: %v", err)
	}
	fmt.Println(chunk2)
}
