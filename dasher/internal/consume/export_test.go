// export_test.go exposes internal fields and methods for white-box tests in
// package consume_test.
package consume

import "context"

// SetAckFn overrides the XACK function for unit tests, allowing injection of
// transient failures.
func SetAckFn(c *Consumer, fn func(ctx context.Context, stream, group, id string) error) {
	c.ackFn = fn
}

// ExposePeerReclaim calls peerReclaim directly for white-box tests.
func ExposePeerReclaim(c *Consumer, ctx context.Context) error {
	return c.peerReclaim(ctx)
}

// ExposeSelfReclaim calls selfReclaim directly for white-box tests.
func ExposeSelfReclaim(c *Consumer, ctx context.Context) error {
	return c.selfReclaim(ctx)
}
