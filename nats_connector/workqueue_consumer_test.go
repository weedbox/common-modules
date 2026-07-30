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

// Shutdown used to be cancel() + an unbounded wg.Wait(). Cancellation is only a
// request, so a handler that never observes its context held Shutdown — and
// therefore the caller's stop hook — forever. These tests pin the bound.

// newShutdownTestConsumer builds a consumer without touching NATS: Shutdown
// only needs ctx/cancel and the WaitGroup that Start populates.
func newShutdownTestConsumer(cfg *WorkQueueConfig) *WorkQueueConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkQueueConsumer{config: cfg, ctx: ctx, cancel: cancel}
}

// wedgeHandler simulates an in-flight handler that ignores its context. The
// returned release func lets it finish so the test doesn't leak the goroutine.
func wedgeHandler(wqc *WorkQueueConsumer) (release func()) {
	stuck := make(chan struct{})
	wqc.wg.Add(1)
	go func() {
		defer wqc.wg.Done()
		<-stuck
	}()
	return func() { close(stuck) }
}

func TestShutdownContext_ReturnsWhenHandlersFinish(t *testing.T) {
	wqc := newShutdownTestConsumer(&WorkQueueConfig{ConsumerName: "test"})

	// A well-behaved handler: unblocks as soon as the consumer ctx is cancelled.
	wqc.wg.Add(1)
	go func() {
		defer wqc.wg.Done()
		<-wqc.ctx.Done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := wqc.ShutdownContext(ctx); err != nil {
		t.Fatalf("ShutdownContext returned %v, want nil", err)
	}
}

func TestShutdownContext_GivesUpOnWedgedHandler(t *testing.T) {
	wqc := newShutdownTestConsumer(&WorkQueueConfig{ConsumerName: "test"})
	release := wedgeHandler(wqc)
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := wqc.ShutdownContext(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("ShutdownContext returned nil, want an error — the handler never finished")
	}
	if elapsed > 3*time.Second {
		t.Errorf("ShutdownContext blocked for %v, want it bounded by the context", elapsed)
	}
}

func TestShutdown_BoundedByConfiguredTimeout(t *testing.T) {
	var reported error
	wqc := newShutdownTestConsumer(&WorkQueueConfig{
		ConsumerName:    "test",
		ShutdownTimeout: 100 * time.Millisecond,
		OnError:         func(err error) { reported = err },
	})
	release := wedgeHandler(wqc)
	defer release()

	done := make(chan struct{})
	go func() {
		wqc.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown never returned — the wait is still unbounded")
	}

	if reported == nil {
		t.Error("the abandoned handler was not reported through OnError")
	}
}

// TestShutdown_NegativeTimeoutWaitsForever pins the documented opt-out: a
// caller that would rather hang than proceed sets a negative timeout.
func TestShutdown_NegativeTimeoutWaitsForever(t *testing.T) {
	wqc := newShutdownTestConsumer(&WorkQueueConfig{
		ConsumerName:    "test",
		ShutdownTimeout: -1,
	})
	release := wedgeHandler(wqc)

	done := make(chan struct{})
	go func() {
		wqc.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("Shutdown returned while a handler was still running, despite the unbounded opt-out")
	case <-time.After(300 * time.Millisecond):
	}

	release()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not return after the handler finished")
	}
}

func TestNewWorkQueueConsumerConfig_HasBoundedShutdown(t *testing.T) {
	if got := NewWorkQueueConsumerConfig().ShutdownTimeout; got != DefaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", got, DefaultShutdownTimeout)
	}
}

// TestShutdown_ZeroTimeoutUsesDefault: callers that build WorkQueueConfig as a
// literal never see the constructor's default, so zero must not mean "wait
// forever" — that is the exact hang this change removes.
func TestShutdown_ZeroTimeoutUsesDefault(t *testing.T) {
	wqc := newShutdownTestConsumer(&WorkQueueConfig{ConsumerName: "test"})
	release := wedgeHandler(wqc)
	defer release()

	// Assert on the resolved value rather than waiting out the default.
	timeout := wqc.config.ShutdownTimeout
	if timeout == 0 {
		timeout = DefaultShutdownTimeout
	}
	if timeout <= 0 {
		t.Fatalf("resolved shutdown timeout = %v, want a positive bound", timeout)
	}
}
