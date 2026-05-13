package nats_connector

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// ---- test harness ----

type testRig struct {
	srv     *serverHandle
	nc      *nats.Conn
	js      jetstream.JetStream
	bucket  string
	logger  *zap.Logger
	ttl     time.Duration
	replica int
}

type serverHandle struct {
	addr    string
	storeDir string
	shutdown func()
}

func newRig(t *testing.T) *testRig {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()

	srv := natsserver.RunServer(&opts)
	if !srv.ReadyForConnections(5 * time.Second) {
		srv.Shutdown()
		t.Fatalf("nats test server not ready")
	}

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		srv.Shutdown()
		t.Fatalf("connect nats: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		t.Fatalf("jetstream.New: %v", err)
	}

	rig := &testRig{
		srv:     &serverHandle{addr: srv.ClientURL(), storeDir: opts.StoreDir, shutdown: srv.Shutdown},
		nc:      nc,
		js:      js,
		bucket:  "test_locks",
		logger:  zap.NewNop(),
		ttl:     2 * time.Second,
		replica: 1,
	}
	t.Cleanup(func() {
		nc.Close()
		srv.Shutdown()
	})
	return rig
}

func (r *testRig) newLock(t *testing.T, cfg LockConfig) *Lock {
	t.Helper()
	if cfg.TTL == 0 {
		cfg.TTL = r.ttl
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = r.ttl / 4
	}
	l, err := newLock(r.js, r.bucket, r.ttl, r.replica, r.logger, cfg)
	if err != nil {
		t.Fatalf("newLock: %v", err)
	}
	return l
}

// ---- tests ----

func TestLock_SingleHolder(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "single"})
	b := r.newLock(t, LockConfig{Key: "single"})

	ok, err := a.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("a TryLock want (true, nil) got (%v, %v)", ok, err)
	}

	ok, err = b.TryLock(ctx)
	if err != nil {
		t.Fatalf("b TryLock err: %v", err)
	}
	if ok {
		t.Fatal("b TryLock should have failed while a holds the lock")
	}

	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a Unlock: %v", err)
	}
}

func TestLock_ReacquireAfterUnlock(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "reacq"})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a Unlock: %v", err)
	}

	b := r.newLock(t, LockConfig{Key: "reacq"})
	ok, err := b.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("b TryLock after unlock want (true, nil) got (%v, %v)", ok, err)
	}
	_ = b.Unlock(ctx)
}

func TestLock_TTLAutoRelease(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// Acquire with the minimum allowed TTL (1s) and a long heartbeat so
	// the lock won't be refreshed before it expires — simulates a
	// crashed holder.
	a := r.newLock(t, LockConfig{Key: "ttl", TTL: time.Second, HeartbeatInterval: time.Hour})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}
	// Don't unlock — simulate a crashed holder. Heartbeat is set to 1h so
	// it won't refresh in time.

	// Another caller should get the lock once TTL expires.
	b := r.newLock(t, LockConfig{Key: "ttl"})
	bctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err := b.Lock(bctx); err != nil {
		t.Fatalf("b Lock (expecting TTL-expired takeover): %v", err)
	}
	_ = b.Unlock(ctx)

	// Now a's Unlock should report ErrLockLost because the lease expired
	// and b took over with a fresh revision.
	err := a.Unlock(ctx)
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("a Unlock after TTL expiry want ErrLockLost, got %v", err)
	}
}

func TestLock_SingleUseGuard(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "single-use"})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}
	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a Unlock: %v", err)
	}

	// Re-acquire on the same instance must fail.
	ok, err := a.TryLock(ctx)
	if err == nil {
		t.Fatal("TryLock on used Lock should error, got nil")
	}
	if ok {
		t.Fatal("TryLock on used Lock should not succeed")
	}
}

func TestLock_UnlockReturnsErrLockLostAfterHeartbeatLoss(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "lost-via-hb", TTL: time.Second, HeartbeatInterval: 150 * time.Millisecond})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}

	// External purge → next heartbeat sees the key gone → handleLost.
	kv, err := r.js.KeyValue(ctx, r.bucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if err := kv.Purge(ctx, lockKey("lost-via-hb")); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	// Wait for heartbeat to fire and detect the loss.
	select {
	case <-a.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("Done() did not close after external purge")
	}

	// Unlock should report ErrLockLost.
	if err := a.Unlock(ctx); !errors.Is(err, ErrLockLost) {
		t.Fatalf("Unlock after heartbeat loss want ErrLockLost, got %v", err)
	}
}

func TestLock_BlocksThenAcquires(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "block"})
	b := r.newLock(t, LockConfig{Key: "block"})

	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}

	bAcquired := make(chan error, 1)
	go func() {
		bAcquired <- b.Lock(ctx)
	}()

	// b should be blocked.
	select {
	case err := <-bAcquired:
		t.Fatalf("b Lock returned too early: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	if err := a.Unlock(ctx); err != nil {
		t.Fatalf("a Unlock: %v", err)
	}

	select {
	case err := <-bAcquired:
		if err != nil {
			t.Fatalf("b Lock: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("b Lock did not acquire after a Unlock")
	}
	_ = b.Unlock(ctx)
}

func TestLock_ContextCancelWhileWaiting(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "ctxcancel"})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}
	defer a.Unlock(ctx)

	b := r.newLock(t, LockConfig{Key: "ctxcancel"})
	bctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- b.Lock(bctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("b Lock after cancel want context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("b Lock did not return after ctx cancel")
	}
}

func TestLock_HeartbeatKeepsAlive(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// TTL 1s, heartbeat every 250ms — should survive past 2.5×TTL.
	a := r.newLock(t, LockConfig{Key: "hb", TTL: time.Second, HeartbeatInterval: 250 * time.Millisecond})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}
	defer a.Unlock(ctx)

	// Wait 2.5×TTL.
	time.Sleep(2500 * time.Millisecond)

	// Another caller should NOT be able to acquire.
	b := r.newLock(t, LockConfig{Key: "hb"})
	ok, err := b.TryLock(ctx)
	if err != nil {
		t.Fatalf("b TryLock err: %v", err)
	}
	if ok {
		t.Fatal("b TryLock should have failed while a is heartbeating")
	}
}

func TestLock_DoneClosedOnExternalPurge(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	a := r.newLock(t, LockConfig{Key: "purge", TTL: time.Second, HeartbeatInterval: 200 * time.Millisecond})
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a Lock: %v", err)
	}

	// Simulate external purge of the lock key.
	kv, err := r.js.KeyValue(ctx, r.bucket)
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	if err := kv.Purge(ctx, lockKey("purge")); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	select {
	case <-a.Done():
		// success: heartbeat noticed the loss
	case <-time.After(3 * time.Second):
		t.Fatal("Done() did not close after external purge")
	}
}

func TestOnce_RunsOnlyOnceUnderContention(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	const N = 8
	var calls int32

	var wg sync.WaitGroup
	wg.Add(N)
	errs := make([]error, N)

	for i := range N {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = runOnce(ctx, r.js, r.bucket, r.ttl, r.replica, r.logger, "init-key", func(ctx context.Context) error {
				atomic.AddInt32(&calls, 1)
				time.Sleep(200 * time.Millisecond)
				return nil
			})
		}(i)
	}
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("fn ran %d times, want 1", got)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d returned error: %v", i, err)
		}
	}
}

func TestOnce_FastPathAfterSuccess(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	called := 0
	err := runOnce(ctx, r.js, r.bucket, r.ttl, r.replica, r.logger, "init-fast", func(ctx context.Context) error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("first Once: %v", err)
	}

	// Second call: should NOT invoke fn.
	err = runOnce(ctx, r.js, r.bucket, r.ttl, r.replica, r.logger, "init-fast", func(ctx context.Context) error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("second Once: %v", err)
	}
	if called != 1 {
		t.Fatalf("fn called %d times, want 1", called)
	}
}

func TestOnce_RetriesWhenFnFails(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	first := errors.New("first failure")

	err := runOnce(ctx, r.js, r.bucket, r.ttl, r.replica, r.logger, "init-retry", func(ctx context.Context) error {
		return first
	})
	if !errors.Is(err, first) {
		t.Fatalf("first Once want %v, got %v", first, err)
	}

	// Second call should retry (done key not written on failure).
	called := 0
	err = runOnce(ctx, r.js, r.bucket, r.ttl, r.replica, r.logger, "init-retry", func(ctx context.Context) error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("second Once: %v", err)
	}
	if called != 1 {
		t.Fatalf("fn called %d times, want 1", called)
	}
}

// Sanity: ensure ownerID default is unique enough across two locks.
func TestLock_DefaultOwnerIDUnique(t *testing.T) {
	a := defaultOwnerID()
	b := defaultOwnerID()
	if a == b {
		t.Fatalf("defaultOwnerID returned same value twice: %s", a)
	}
	if a == "" || b == "" {
		t.Fatal("defaultOwnerID returned empty")
	}
	_ = fmt.Sprintf("debug: %s vs %s", a, b)
}

// Verifies ensureLockBucket falls back to replicas=1 when the connected
// server can't honour the requested replica count. The embedded test
// server runs in non-clustered mode, so requesting replicas=3 must
// trigger the fallback path and still succeed.
func TestEnsureLockBucket_FallbackOnSingleMode(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	kv, err := ensureLockBucket(ctx, r.js, "fallback_locks", 3)
	if err != nil {
		t.Fatalf("ensureLockBucket(replicas=3) on single-mode server: %v", err)
	}
	if kv == nil {
		t.Fatal("ensureLockBucket returned nil KV")
	}

	status, err := kv.Status(ctx)
	if err != nil {
		t.Fatalf("kv.Status: %v", err)
	}
	if got := status.Config().Replicas; got != 1 {
		t.Fatalf("bucket replicas after fallback want 1, got %d", got)
	}
}

func TestIsReplicasUnsupportedErr(t *testing.T) {
	matches := []string{
		"replicas > 1 not supported in non-clustered mode",
		"insufficient resources",
		"no suitable peers for placement",
		"JetStream cluster not available",
		"JetStream clustering required",
	}
	for _, m := range matches {
		if !matchesReplicaUnsupportedMsg(m) {
			t.Errorf("expected match for %q", m)
		}
	}

	nonMatches := []string{
		"",
		"stream name required",
		"consumer not found",
		"bad request",
		"context deadline exceeded",
		"key already exists",
	}
	for _, m := range nonMatches {
		if matchesReplicaUnsupportedMsg(m) {
			t.Errorf("did not expect match for %q", m)
		}
	}

	// Wrapped APIError path.
	apiErr := &jetstream.APIError{
		Code:        500,
		ErrorCode:   10052,
		Description: "replicas > 1 not supported in non-clustered mode",
	}
	wrapped := fmt.Errorf("create kv: %w", apiErr)
	if !isReplicasUnsupportedErr(wrapped) {
		t.Errorf("isReplicasUnsupportedErr should match wrapped APIError")
	}

	if isReplicasUnsupportedErr(nil) {
		t.Errorf("isReplicasUnsupportedErr(nil) must be false")
	}
}
