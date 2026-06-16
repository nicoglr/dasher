package middleware_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/middleware"
)

func TestEmitAfter_InnerSuccess_Emits(t *testing.T) {
	inner := &captureHandler{}
	prod := &fakeProducer{}
	h := middleware.EmitAfter(prod, "downstream", inner)

	evt := baseEvent()
	err := h.Handle(context.Background(), dasher.InstanceContext{}, evt)
	require.NoError(t, err)
	assert.True(t, inner.called)
	assert.True(t, prod.called)
}

func TestEmitAfter_InnerError_NoEmit(t *testing.T) {
	inner := &captureHandler{err: errors.New("side effect failed")}
	prod := &fakeProducer{}
	h := middleware.EmitAfter(prod, "downstream", inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.False(t, prod.called)
}

func TestEmitAfter_ProducerError_Returned(t *testing.T) {
	inner := &captureHandler{}
	prod := &fakeProducer{err: errors.New("redis down")}
	h := middleware.EmitAfter(prod, "downstream", inner)

	err := h.Handle(context.Background(), dasher.InstanceContext{}, baseEvent())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis down")
}
