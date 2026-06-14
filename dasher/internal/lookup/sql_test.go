package lookup_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"4gclinical.com/dasher/internal/lookup"
)

func TestSQLLookup_NoRows(t *testing.T) {
	l, _ := lookup.NewSQLLookupForTest(nil, nil, "SELECT email FROM users WHERE id = @id")
	rows, err := l.Resolve(context.Background(), map[string]any{"id": int64(1)})
	require.NoError(t, err)
	assert.Len(t, rows, 0)
}

func TestSQLLookup_SingleRow(t *testing.T) {
	preset := []lookup.Row{{"email": "alice@example.com"}}
	l, _ := lookup.NewSQLLookupForTest(preset, nil, "SELECT email FROM users WHERE id = @id")
	rows, err := l.Resolve(context.Background(), map[string]any{"id": int64(1)})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "alice@example.com", rows[0]["email"])
}

func TestSQLLookup_MultiRow(t *testing.T) {
	preset := []lookup.Row{{"email": "a@x.com"}, {"email": "b@x.com"}}
	l, _ := lookup.NewSQLLookupForTest(preset, nil, "SELECT email FROM users WHERE group_id = @group_id")
	rows, err := l.Resolve(context.Background(), map[string]any{"group_id": int64(5)})
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestSQLLookup_BindReachesQuery(t *testing.T) {
	preset := []lookup.Row{{"email": "alice@example.com"}}
	l, fq := lookup.NewSQLLookupForTest(preset, nil, "SELECT email FROM users WHERE id = @role_id")
	_, err := l.Resolve(context.Background(), map[string]any{"role_id": int64(42)})
	require.NoError(t, err)
	assert.Equal(t, int64(42), fq.LastArgs["role_id"])
}

// TestSQLLookup_RealPG runs only when DASHER_PG_DSN is set (real Postgres).
// It verifies that a json.Number-normalised int64 bind value round-trips
// correctly through pgx.NamedArgs to an integer PK (oracle B2).
func TestSQLLookup_RealPG(t *testing.T) {
	dsn := os.Getenv("DASHER_PG_DSN")
	if dsn == "" {
		t.Skip("DASHER_PG_DSN not set — skipping real-PG bind test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// Create a temp table with an integer PK and insert a row.
	_, err = pool.Exec(ctx, `CREATE TEMP TABLE _dasher_bind_test (id INTEGER PRIMARY KEY, label TEXT)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO _dasher_bind_test VALUES (1, 'hello')`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DROP TABLE IF EXISTS _dasher_bind_test`)
	})

	spec := lookup.Spec{
		Type: lookup.TypeSQL,
		Raw:  map[string]any{"sql": "SELECT label FROM _dasher_bind_test WHERE id = @id"},
	}
	deps := lookup.Deps{Pool: pool}
	l, err := lookup.DefaultRegistry[lookup.TypeSQL](spec, deps)
	require.NoError(t, err)

	// Bind with a normalised int64 (simulates what ResolveBind returns for json.Number "1").
	rows, err := l.Resolve(ctx, map[string]any{"id": int64(1)})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "hello", rows[0]["label"])
}
