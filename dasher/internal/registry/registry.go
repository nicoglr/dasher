// Package registry maps handler names (as listed in config) to dasher.Handler
// implementations. Names are opaque keys — "@v1" suffixes are not parsed in
// v0. This name-based indirection is the wazero seam: a future registry can
// resolve a name to a dynamically-loaded wasm module instead of a compiled-in
// Go function.
package registry

import (
	"4gclinical.com/dasher"
	"4gclinical.com/dasher/internal/handlers"
)

// Registry maps handler names to Handler implementations.
type Registry map[string]dasher.Handler

// Lookup returns the handler for the given name and whether it was found.
func (r Registry) Lookup(name string) (dasher.Handler, bool) {
	h, ok := r[name]
	return h, ok
}

// Default returns the v0 handler registry binding all known handler names to
// the compiled-in Forward implementation.
func Default() Registry {
	internal := dasher.HandlerFunc(handlers.Forward(handlers.ServiceInternal))
	gateway := dasher.HandlerFunc(handlers.Forward(handlers.ServiceGateway))
	return Registry{
		"order-sync@v1": internal,
		"order-sync@v2": internal,
		"product-sync":  internal,
		"billing-sync":  internal,
		"gateway-sync":  gateway,
	}
}
