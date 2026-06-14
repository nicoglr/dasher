// export_test.go exposes internal constructors for white-box tests in package lookup.
package lookup

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// FakeRowQuerier is a test-only rowQuerier that returns preset rows or error.
// Exported so sql_test.go (package lookup_test) can inspect captured args.
type FakeRowQuerier struct {
	Rows     []Row
	Err      error
	LastSQL  string
	LastArgs pgx.NamedArgs
}

func (f *FakeRowQuerier) QueryRows(_ context.Context, sql string, args pgx.NamedArgs) ([]Row, error) {
	f.LastSQL = sql
	f.LastArgs = args
	return f.Rows, f.Err
}

// NewSQLLookupForTest creates an sqlLookup backed by a FakeRowQuerier.
func NewSQLLookupForTest(rows []Row, errVal error, sqlText string) (Lookup, *FakeRowQuerier) {
	fq := &FakeRowQuerier{Rows: rows, Err: errVal}
	l, _ := newSQLLookup(fq, sqlText, 0)
	return l, fq
}
