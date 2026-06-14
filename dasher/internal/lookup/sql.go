package lookup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultQueryTimeout = 5 * time.Second

// rowQuerier runs a named-args query and returns result rows as plain maps.
// This narrow interface enables unit tests to inject a fake without implementing
// the full pgx.Rows interface.
type rowQuerier interface {
	QueryRows(ctx context.Context, sql string, args pgx.NamedArgs) ([]Row, error)
}

// poolQuerier adapts *pgxpool.Pool to the rowQuerier interface.
type poolQuerier struct{ pool *pgxpool.Pool }

func (p *poolQuerier) QueryRows(ctx context.Context, sql string, args pgx.NamedArgs) ([]Row, error) {
	rows, err := p.pool.Query(ctx, sql, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil, err
	}
	out := make([]Row, len(result))
	for i, r := range result {
		out[i] = Row(r)
	}
	return out, nil
}

// sqlLookup implements Lookup via pgx NamedArgs queries.
type sqlLookup struct {
	q       rowQuerier
	sql     string
	timeout time.Duration
}

// newSQLLookup creates an sqlLookup. Returns an error if sql is empty.
func newSQLLookup(q rowQuerier, sql string, timeout time.Duration) (*sqlLookup, error) {
	if sql == "" {
		return nil, errors.New("lookup sql: sql text is required")
	}
	if timeout <= 0 {
		timeout = defaultQueryTimeout
	}
	return &sqlLookup{q: q, sql: sql, timeout: timeout}, nil
}

// Resolve executes the SQL query with bind values supplied as pgx.NamedArgs.
func (l *sqlLookup) Resolve(ctx context.Context, bind map[string]any) ([]Row, error) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()

	rows, err := l.q.QueryRows(ctx, l.sql, pgx.NamedArgs(bind))
	if err != nil {
		return nil, fmt.Errorf("sql lookup query: %w", err)
	}
	return rows, nil
}

func init() {
	DefaultRegistry.Register(TypeSQL, func(spec Spec, deps Deps) (Lookup, error) {
		sqlText, _ := spec.Raw["sql"].(string)
		if deps.Pool == nil {
			return nil, errors.New("sql lookup: db pool is required")
		}
		return newSQLLookup(&poolQuerier{pool: deps.Pool}, sqlText, 0)
	})
}
