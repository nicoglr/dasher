package lookup

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TypeSQL is the registered type name for SQL lookups.
const TypeSQL = "sql"

// Row is a single lookup result row: a map of column → value.
type Row = map[string]any

// Lookup executes a parameterised lookup query for a given set of bind values.
type Lookup interface {
	// Resolve runs the lookup for the given bind parameters.
	// Returns the result rows, or an error.
	Resolve(ctx context.Context, bind map[string]any) ([]Row, error)
}

// Spec is a parsed catalog entry.
type Spec struct {
	// Type is the lookup type discriminator (e.g. TypeSQL).
	Type string
	// TTL is the cache TTL. Zero means no caching.
	TTL time.Duration
	// Raw holds the type-specific fields (e.g. "sql" key for SQL text).
	Raw map[string]any
}

// Deps carries the runtime dependencies available to lookup factories.
type Deps struct {
	Pool *pgxpool.Pool
}

// Factory is a constructor for a Lookup implementation.
// Called once per catalog entry at startup.
type Factory func(spec Spec, deps Deps) (Lookup, error)

// Registry maps type names to their factories.
// Use Register to populate.
type Registry map[string]Factory

// Register adds a factory for typeName.
func (r Registry) Register(typeName string, f Factory) {
	r[typeName] = f
}

// DefaultRegistry is the global type registry populated by init() functions
// in lookup sub-packages (e.g. sql.go).
var DefaultRegistry = Registry{}
