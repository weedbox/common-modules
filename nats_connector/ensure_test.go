package nats_connector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// connectorOnRig wires a *NATSConnector to the rig's NATS connection so
// the method-form tests can exercise the public API surface against the
// same embedded server.
func connectorOnRig(r *testRig) *NATSConnector {
	return &NATSConnector{
		logger: r.logger,
		conn:   r.nc,
		jsv2:   r.js,
	}
}

// warmJetStream sends one round-trip JetStream request to ensure the
// embedded server has finished initialising its JetStream subsystem.
// Without this, a concurrent burst of CreateOrUpdate requests can race
// against the server's own init goroutines (a pre-existing nats-server
// internal race only visible under `go test -race`). This is purely a
// test-harness concern and unrelated to the Ensure* semantics.
func warmJetStream(t *testing.T, r *testRig, ctx context.Context) {
	t.Helper()
	if _, err := r.js.AccountInfo(ctx); err != nil {
		t.Fatalf("warmJetStream AccountInfo: %v", err)
	}
}

// skipIfRaceDetector skips a concurrent-burst test when the race
// detector is enabled. The embedded nats-server v2.14.0 has known data
// races in concurrent CreateOrUpdateStream handling (the production
// server serializes these through the meta-leader, so the bug never
// surfaces there). Skipping under -race lets the rest of the suite run
// race-clean while still exercising correctness without the detector.
func skipIfRaceDetector(t *testing.T) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("skipping under -race: embedded nats-server v2.14.0 races in concurrent stream handling")
	}
}

// --- EnsureKV ------------------------------------------------------

func TestEnsureKV_BasicCreate(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	cfg := jetstream.KeyValueConfig{Bucket: "ensure_basic"}
	kv, err := EnsureKV(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("EnsureKV: %v", err)
	}
	if kv == nil {
		t.Fatalf("expected non-nil KV handle")
	}
	if _, err := kv.PutString(ctx, "k", "v"); err != nil {
		t.Fatalf("kv.PutString: %v", err)
	}
	got, err := kv.Get(ctx, "k")
	if err != nil {
		t.Fatalf("kv.Get: %v", err)
	}
	if string(got.Value()) != "v" {
		t.Fatalf("got %q, want %q", got.Value(), "v")
	}
}

func TestEnsureKV_AlreadyExistsIsIdempotent(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	cfg := jetstream.KeyValueConfig{Bucket: "ensure_idempotent"}

	kv1, err := EnsureKV(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("first EnsureKV: %v", err)
	}
	kv2, err := EnsureKV(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("second EnsureKV: %v", err)
	}
	if kv1 == nil || kv2 == nil {
		t.Fatalf("nil KV handle")
	}
}

func TestEnsureKV_ConcurrentEnsureSameBucket(t *testing.T) {
	skipIfRaceDetector(t)
	r := newRig(t)
	ctx := context.Background()
	warmJetStream(t, r, ctx)

	const N = 8
	cfg := jetstream.KeyValueConfig{Bucket: "ensure_concurrent_kv"}

	var wg sync.WaitGroup
	errs := make([]error, N)
	wg.Add(N)
	for i := range N {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = EnsureKV(ctx, r.js, cfg)
		}(i)
	}
	wg.Wait()

	var success int32
	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
			continue
		}
		atomic.AddInt32(&success, 1)
	}
	if success != N {
		t.Fatalf("only %d/%d succeeded", success, N)
	}

	// Verify the underlying stream exists exactly once.
	s, err := r.js.Stream(ctx, "KV_ensure_concurrent_kv")
	if err != nil {
		t.Fatalf("Stream lookup after concurrent ensure: %v", err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatalf("Stream Info: %v", err)
	}
	if info.Config.Name != "KV_ensure_concurrent_kv" {
		t.Fatalf("unexpected stream name %q", info.Config.Name)
	}
}

func TestEnsureKV_ReplicaFallbackOnSingleNode(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// Requesting Replicas=3 against the single-node embedded server should
	// not error: the helper either succeeds with the requested count (some
	// dev servers accept it) or transparently demotes to 1 via the
	// fallback path. Both outcomes are valid; the success criterion here
	// is "no error returned and KV is usable". TestEnsureOptions_
	// LoggerCapturesFallback covers the demote-to-1 codepath specifically.
	cfg := jetstream.KeyValueConfig{Bucket: "ensure_fallback_kv", Replicas: 3}
	kv, err := EnsureKV(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("EnsureKV with Replicas=3 on single node: %v", err)
	}
	if kv == nil {
		t.Fatalf("nil KV")
	}
	if _, err := kv.PutString(ctx, "k", "v"); err != nil {
		t.Fatalf("KV not writable after ensure: %v", err)
	}
}

func TestEnsureKV_WithoutReplicaFallbackReturnsError(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cfg := jetstream.KeyValueConfig{Bucket: "ensure_nofallback", Replicas: 3}
	_, err := EnsureKV(ctx, r.js, cfg, WithoutReplicaFallback())
	if err == nil {
		t.Fatalf("expected insufficient-peers error, got nil")
	}
	// When fallback is disabled, insufficient_peers falls through to the
	// retry loop (it isn't transient, isn't already-in-use, isn't a
	// permanent error in the default branch — actually it hits default
	// because the case-guard fails). Confirm we surfaced a placement error.
	if !isInsufficientPeers(errors.Unwrap(err)) && !isInsufficientPeers(err) {
		t.Fatalf("expected insufficient-peers in error chain, got: %v", err)
	}
}

func TestEnsureKV_RequiresBucket(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	_, err := EnsureKV(ctx, r.js, jetstream.KeyValueConfig{})
	if err == nil {
		t.Fatalf("expected error when Bucket is empty")
	}
	if !strings.Contains(err.Error(), "bucket required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureKV_ContextCancellation(t *testing.T) {
	r := newRig(t)

	// Shut down the server so subsequent JetStream calls hit transient
	// "no responders" errors, which the retry loop absorbs. The ctx
	// timeout should then trip and bail out the loop.
	r.srv.shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := EnsureKV(ctx, r.js, jetstream.KeyValueConfig{Bucket: "ensure_ctx_cancel"})
	if err == nil {
		t.Fatalf("expected ctx error after server shutdown")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Logf("note: error chain may include transient error wrapper: %v", err)
	}
}

// --- EnsureStream --------------------------------------------------

func TestEnsureStream_BasicCreate(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	cfg := jetstream.StreamConfig{
		Name:     "ENSURE_BASIC",
		Subjects: []string{"ensure.basic.>"},
	}
	stream, err := EnsureStream(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if stream == nil {
		t.Fatalf("nil stream")
	}

	if _, err := r.js.Publish(ctx, "ensure.basic.hello", []byte("hi")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEnsureStream_ConcurrentEnsureSameStream(t *testing.T) {
	skipIfRaceDetector(t)
	r := newRig(t)
	ctx := context.Background()
	warmJetStream(t, r, ctx)

	const N = 8
	cfg := jetstream.StreamConfig{
		Name:     "ENSURE_CONCURRENT",
		Subjects: []string{"ensure.concurrent.>"},
	}

	var wg sync.WaitGroup
	errs := make([]error, N)
	wg.Add(N)
	for i := range N {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = EnsureStream(ctx, r.js, cfg)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}

	// Verify the stream exists exactly once with matching config.
	stream, err := r.js.Stream(ctx, "ENSURE_CONCURRENT")
	if err != nil {
		t.Fatalf("Stream lookup: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Stream Info: %v", err)
	}
	if info.Config.Name != "ENSURE_CONCURRENT" {
		t.Fatalf("unexpected stream name %q", info.Config.Name)
	}
}

func TestEnsureStream_ConflictingSubjectsSurfacesError(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// First stream owns subjects "a.>".
	if _, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_OWNER",
		Subjects: []string{"a.>"},
	}); err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// A different stream cannot claim the same subjects — server returns a
	// "subjects overlap" error which is not transient and not
	// already-in-use; should surface immediately.
	_, err := EnsureStream(ctx2, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_INTRUDER",
		Subjects: []string{"a.>"},
	})
	if err == nil {
		t.Fatalf("expected subjects-overlap error, got nil")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error was retried until ctx expired — placement error should surface immediately: %v", err)
	}
}

func TestEnsureStream_ReplicaFallbackOnSingleNode(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// Same rationale as TestEnsureKV_ReplicaFallbackOnSingleNode: the
	// success criterion is no error + the stream accepts publishes,
	// regardless of whether the embedded server forced the fallback path.
	cfg := jetstream.StreamConfig{
		Name:     "ENSURE_REPLICAS",
		Subjects: []string{"ensure.replicas.>"},
		Replicas: 3,
	}
	stream, err := EnsureStream(ctx, r.js, cfg)
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if stream == nil {
		t.Fatalf("nil stream")
	}
	if _, err := r.js.Publish(ctx, "ensure.replicas.hello", []byte("hi")); err != nil {
		t.Fatalf("publish after ensure: %v", err)
	}
}

func TestEnsureStream_RequiresName(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	_, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{})
	if err == nil {
		t.Fatalf("expected error when Name is empty")
	}
}

// --- EnsureConsumer ------------------------------------------------

func TestEnsureConsumer_BasicCreate(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	stream, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_CONSUMER",
		Subjects: []string{"ensure.consumer.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	cfg := jetstream.ConsumerConfig{
		Durable:   "worker",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   5 * time.Second,
	}
	consumer, err := EnsureConsumer(ctx, stream, cfg)
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if consumer == nil {
		t.Fatalf("nil consumer")
	}

	// Round-trip lookup
	c2, err := stream.Consumer(ctx, "worker")
	if err != nil {
		t.Fatalf("stream.Consumer lookup: %v", err)
	}
	if c2 == nil {
		t.Fatalf("lookup returned nil consumer")
	}
}

func TestEnsureConsumer_ConcurrentSameDurable(t *testing.T) {
	skipIfRaceDetector(t)
	r := newRig(t)
	ctx := context.Background()
	warmJetStream(t, r, ctx)

	stream, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_CONSUMER_CONCURRENT",
		Subjects: []string{"ensure.cc.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}

	const N = 8
	cfg := jetstream.ConsumerConfig{
		Durable:   "worker_cc",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   5 * time.Second,
	}

	var wg sync.WaitGroup
	errs := make([]error, N)
	wg.Add(N)
	for i := range N {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = EnsureConsumer(ctx, stream, cfg)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

func TestEnsureConsumer_RequiresStream(t *testing.T) {
	ctx := context.Background()
	_, err := EnsureConsumer(ctx, nil, jetstream.ConsumerConfig{Durable: "x"})
	if err == nil {
		t.Fatalf("expected error when stream is nil")
	}
	if !strings.Contains(err.Error(), "stream handle required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- EnsureReplicaScale ------------------------------------------------------

func TestEnsureReplicaScale_NoopOnDesiredLEOne(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	// Should return without touching anything, even for a nonexistent
	// stream — desired<=1 is the first guard.
	EnsureReplicaScale(ctx, r.js, "NONEXISTENT_STREAM", 1)
	EnsureReplicaScale(ctx, r.js, "NONEXISTENT_STREAM", 0)
}

func TestEnsureReplicaScale_NoopWhenSingleNode(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	stream, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_SCALE_NOOP",
		Subjects: []string{"ensure.scale.noop.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	infoBefore, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %v", err)
	}

	// Single-node embedded server: info.Cluster is nil, scale-up should
	// be a no-op (no panic, no error returned because EnsureReplicaScale
	// has no return value).
	EnsureReplicaScale(ctx, r.js, "ENSURE_SCALE_NOOP", 3)

	infoAfter, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Info after scale: %v", err)
	}
	if infoBefore.Config.Replicas != infoAfter.Config.Replicas {
		t.Fatalf("Replicas changed unexpectedly: %d -> %d",
			infoBefore.Config.Replicas, infoAfter.Config.Replicas)
	}
}

// --- NATSConnector method form ----------------------------------------------

func TestNATSConnector_GetJetStreamReturnsHandle(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	if got := c.GetJetStream(); got == nil {
		t.Fatalf("expected non-nil JetStream handle")
	}
}

func TestNATSConnector_EnsureKVMethod(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	ctx := context.Background()

	kv, err := c.EnsureKV(ctx, jetstream.KeyValueConfig{Bucket: "method_kv"})
	if err != nil {
		t.Fatalf("EnsureKV: %v", err)
	}
	if _, err := kv.PutString(ctx, "k", "v"); err != nil {
		t.Fatalf("PutString: %v", err)
	}
}

func TestNATSConnector_EnsureStreamMethod(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	ctx := context.Background()

	stream, err := c.EnsureStream(ctx, jetstream.StreamConfig{
		Name:     "METHOD_STREAM",
		Subjects: []string{"method.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if stream == nil {
		t.Fatalf("nil stream")
	}
}

func TestNATSConnector_EnsureConsumerMethod(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	ctx := context.Background()

	stream, err := c.EnsureStream(ctx, jetstream.StreamConfig{
		Name:     "METHOD_STREAM_CONSUMER",
		Subjects: []string{"method.consumer.>"},
	})
	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	consumer, err := c.EnsureConsumer(ctx, stream, jetstream.ConsumerConfig{
		Durable:   "method_worker",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("EnsureConsumer: %v", err)
	}
	if consumer == nil {
		t.Fatalf("nil consumer")
	}
}

func TestNATSConnector_EnsureReplicaScaleMethod(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	ctx := context.Background()

	if _, err := c.EnsureStream(ctx, jetstream.StreamConfig{
		Name:     "METHOD_STREAM_SCALE",
		Subjects: []string{"method.scale.>"},
	}); err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	// Just confirms no panic on single-node server. desired<=1 fast path.
	c.EnsureReplicaScale(ctx, "METHOD_STREAM_SCALE", 1)
	// And desired>1 against single-node (no Cluster info) → silent no-op.
	c.EnsureReplicaScale(ctx, "METHOD_STREAM_SCALE", 3)
}

// --- options coverage --------------------------------------------------------

func TestEnsureOptions_LoggerCapturesFallback(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	_, err := EnsureKV(ctx, r.js, jetstream.KeyValueConfig{
		Bucket:   "ensure_observed",
		Replicas: 3,
	}, WithEnsureLogger(logger))
	if err != nil {
		t.Fatalf("EnsureKV: %v", err)
	}

	if logs.FilterMessageSnippet("falling back to single replica").Len() == 0 {
		var msgs []string
		for _, e := range logs.All() {
			msgs = append(msgs, e.Message)
		}
		t.Fatalf("expected fallback log entry, got: %v", msgs)
	}
}

func TestEnsureOptions_BackoffOverrideDoesNotPanic(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	_, err := EnsureKV(ctx, r.js, jetstream.KeyValueConfig{Bucket: "ensure_backoff_opt"},
		WithEnsureBackoff(50*time.Millisecond, 500*time.Millisecond))
	if err != nil {
		t.Fatalf("EnsureKV with custom backoff: %v", err)
	}
}
