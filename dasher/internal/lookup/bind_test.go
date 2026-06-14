package lookup_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"4gclinical.com/dasher/internal/lookup"
)

func TestResolveBind_DataHit(t *testing.T) {
	data := map[string]any{"id": "alice"}
	old := map[string]any{"id": "bob"}
	assert.Equal(t, "alice", lookup.ResolveBind("id", data, old))
}

func TestResolveBind_OldFallback(t *testing.T) {
	data := map[string]any{"other": "x"}
	old := map[string]any{"id": "bob"}
	assert.Equal(t, "bob", lookup.ResolveBind("id", data, old))
}

func TestResolveBind_BothNil(t *testing.T) {
	assert.Nil(t, lookup.ResolveBind("id", nil, nil))
}

func TestResolveBind_BothMissingKey(t *testing.T) {
	data := map[string]any{"other": "x"}
	old := map[string]any{"other": "y"}
	assert.Nil(t, lookup.ResolveBind("id", data, old))
}

func TestResolveBind_JsonNumberIntegral(t *testing.T) {
	data := map[string]any{"id": json.Number("42")}
	v := lookup.ResolveBind("id", data, nil)
	assert.Equal(t, int64(42), v)
}

func TestResolveBind_JsonNumberFractional(t *testing.T) {
	data := map[string]any{"score": json.Number("3.14")}
	v := lookup.ResolveBind("score", data, nil)
	assert.Equal(t, 3.14, v)
}

func TestResolveBind_JsonNumberNonNumeric(t *testing.T) {
	// json.Number that is not actually a number — passes raw string
	data := map[string]any{"weird": json.Number("not-a-number")}
	v := lookup.ResolveBind("weird", data, nil)
	assert.Equal(t, "not-a-number", v)
}

func TestValidColumn(t *testing.T) {
	assert.True(t, lookup.ValidColumn("user_id"))
	assert.True(t, lookup.ValidColumn("_x"))
	assert.True(t, lookup.ValidColumn("A1"))
	assert.False(t, lookup.ValidColumn(""))
	assert.False(t, lookup.ValidColumn("1bad"))
	assert.False(t, lookup.ValidColumn("bad-key"))
}
