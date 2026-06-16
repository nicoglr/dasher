package registry_test

import (
	"testing"

	"4gclinical.com/dasher/internal/registry"
)

func TestDefaultLookup(t *testing.T) {
	r := registry.Default()
	for _, name := range []string{"order-sync@v1", "order-sync@v2", "product-sync", "billing-sync", "gateway-sync"} {
		if _, ok := r.Lookup(name); !ok {
			t.Errorf("expected handler for %q", name)
		}
	}
	if _, ok := r.Lookup("nonexistent"); ok {
		t.Error("expected no handler for 'nonexistent'")
	}
}
