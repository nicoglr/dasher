package middleware_test

import (
	"context"

	"4gclinical.com/dasher"
)

type captureHandler struct {
	called bool
	last   dasher.Event
	err    error
}

func (h *captureHandler) Handle(_ context.Context, _ dasher.InstanceContext, evt dasher.Event) error {
	h.called = true
	h.last = evt
	return h.err
}

type fakeProducer struct {
	called bool
	last   dasher.Event
	err    error
}

func (p *fakeProducer) Emit(_ context.Context, _ string, evt dasher.Event) error {
	p.called = true
	p.last = evt
	return p.err
}

func baseEvent() dasher.Event {
	return dasher.Event{
		Op:    "insert",
		Table: "users",
		LSN:   "0/1",
		Data:  map[string]any{"id": "alice"},
	}
}
