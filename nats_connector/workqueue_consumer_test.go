package nats_connector

import (
	"context"
	"testing"
	"time"
)

// handlerContext is the single place where a per-invocation handler context is
// derived. These tests lock the opt-in contract: HandlerTimeout == 0 keeps the
// original deadline-less behaviour (so existing callers are unaffected), while
// HandlerTimeout > 0 bounds the handler. Both must always be cancelled when the
// consumer-level ctx (Shutdown) is cancelled.

func TestHandlerContext_ZeroTimeout_NoDeadline(t *testing.T) {
	wqc := &WorkQueueConsumer{
		config: &WorkQueueConfig{HandlerTimeout: 0},
		ctx:    context.Background(),
	}

	ctx, cancel := wqc.handlerContext()
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatalf("HandlerTimeout=0 must not attach a deadline (legacy behaviour)")
	}
}

func TestHandlerContext_PositiveTimeout_HasDeadline(t *testing.T) {
	const timeout = 250 * time.Millisecond
	wqc := &WorkQueueConsumer{
		config: &WorkQueueConfig{HandlerTimeout: timeout},
		ctx:    context.Background(),
	}

	before := time.Now()
	ctx, cancel := wqc.handlerContext()
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatalf("HandlerTimeout>0 must attach a deadline")
	}
	// The deadline should sit roughly timeout into the future.
	got := deadline.Sub(before)
	if got < timeout || got > timeout+time.Second {
		t.Fatalf("deadline %v not within expected window (~%v)", got, timeout)
	}
}

func TestHandlerContext_ShutdownCancelsChild(t *testing.T) {
	// Regardless of HandlerTimeout, cancelling the consumer ctx (Shutdown)
	// must propagate to the derived handler context.
	for _, timeout := range []time.Duration{0, time.Hour} {
		parent, shutdown := context.WithCancel(context.Background())
		wqc := &WorkQueueConsumer{
			config: &WorkQueueConfig{HandlerTimeout: timeout},
			ctx:    parent,
		}

		ctx, cancel := wqc.handlerContext()

		shutdown() // simulate Shutdown()
		select {
		case <-ctx.Done():
			// expected
		case <-time.After(time.Second):
			t.Fatalf("HandlerTimeout=%v: derived ctx not cancelled on shutdown", timeout)
		}
		cancel()
	}
}
