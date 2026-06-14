package lookup_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

type fakeRegistryLookup struct {
	rows []lookup.Row
	err  error
}

func (f *fakeRegistryLookup) Resolve(_ context.Context, _ map[string]any) ([]lookup.Row, error) {
	return f.rows, f.err
}

func TestRegistryBuildKnownType(t *testing.T) {
	reg := lookup.New()
	reg.Register("fake", func(_ lookup.Spec, _ lookup.Deps) (lookup.Lookup, error) {
		return &fakeRegistryLookup{rows: []lookup.Row{{"id": 1}}}, nil
	})

	l, err := reg.Build(lookup.Spec{Type: "fake"}, lookup.Deps{})
	require.NoError(t, err)
	rows, err := l.Resolve(context.Background(), nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestRegistryBuildUnknownType(t *testing.T) {
	reg := lookup.New()
	_, err := reg.Build(lookup.Spec{Type: "nope"}, lookup.Deps{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")
}

func TestRegistryBuildFactoryError(t *testing.T) {
	reg := lookup.New()
	reg.Register("bad", func(_ lookup.Spec, _ lookup.Deps) (lookup.Lookup, error) {
		return nil, errors.New("factory failed")
	})
	_, err := reg.Build(lookup.Spec{Type: "bad"}, lookup.Deps{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "factory failed")
}
