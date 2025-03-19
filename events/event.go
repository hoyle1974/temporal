package events

import "time"

type Event struct {
	Timestamp time.Time
	Key       string
	Data      []byte
	Delete    bool
}

func (e Event) Apply(ret map[string][]byte) {
	if e.Delete {
		delete(ret, e.Key)
	} else {
		ret[e.Key] = e.Data
	}
}
