package nats_connector

// The convergent loop confirms a create by looping back to its own
// lookup. It used to sleep the full retry backoff first, which charged
// every first-time stream and bucket one baseBackoff for nothing — a
// caller provisioning ~30 resources at startup paid ~6s of pure sleep.
//
// These tests pin the create path against a deliberately huge backoff:
// if the successful-create branch ever starts sleeping again, the call
// cannot finish inside the deadline asserted here.

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const hugeBackoff = 5 * time.Second

func TestEnsureStream_SuccessfulCreateDoesNotSleepBackoff(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	start := time.Now()
	stream, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_FAST_CREATE",
		Subjects: []string{"ensure.fastcreate.>"},
		Replicas: 1,
	}, WithEnsureBackoff(hugeBackoff, hugeBackoff))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("EnsureStream: %v", err)
	}
	if stream == nil {
		t.Fatalf("nil stream")
	}
	if elapsed >= hugeBackoff {
		t.Fatalf("first create paid the retry backoff: took %v with base backoff %v", elapsed, hugeBackoff)
	}

	// The returned handle must still be the confirmed, publishable one —
	// skipping the sleep must not skip the confirmation.
	if _, err := r.js.Publish(ctx, "ensure.fastcreate.hello", []byte("hi")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

func TestEnsureKV_SuccessfulCreateDoesNotSleepBackoff(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	start := time.Now()
	kv, err := EnsureKV(ctx, r.js, jetstream.KeyValueConfig{
		Bucket:   "ensure_fast_create",
		Replicas: 1,
	}, WithEnsureBackoff(hugeBackoff, hugeBackoff))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("EnsureKV: %v", err)
	}
	if kv == nil {
		t.Fatalf("nil kv")
	}
	if elapsed >= hugeBackoff {
		t.Fatalf("first create paid the retry backoff: took %v with base backoff %v", elapsed, hugeBackoff)
	}

	if _, err := kv.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
}

// The skip is scoped to the success branch, not to the loop: a create
// that keeps failing must still be spaced out by the backoff. Placement
// against a single-node server with a large insufficient-peers budget
// drives exactly that path.
func TestEnsureStream_FailedCreateStillSleepsBackoff(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := EnsureStream(ctx, r.js, jetstream.StreamConfig{
		Name:     "ENSURE_SLOW_RETRY",
		Subjects: []string{"ensure.slowretry.>"},
		Replicas: 3,
	}, WithEnsureBackoff(time.Second, time.Second), WithInsufficientPeersBudget(time.Minute))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected the call to run out of ctx while retrying placement")
	}
	// Three seconds of ctx at a 1s backoff is a handful of attempts;
	// without the sleep the loop would spin thousands of times.
	if elapsed < time.Second {
		t.Fatalf("retry loop did not back off: gave up after %v", elapsed)
	}
}

// The method form must forward caller options on top of the connector's
// own defaults.
func TestNATSConnector_EnsureStreamMethodForwardsOptions(t *testing.T) {
	r := newRig(t)
	c := connectorOnRig(r)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	_, err := c.EnsureStream(ctx, jetstream.StreamConfig{
		Name:     "ENSURE_METHOD_OPTS",
		Subjects: []string{"ensure.methodopts.>"},
		Replicas: 3,
	}, WithEnsureBackoff(time.Second, time.Second), WithInsufficientPeersBudget(time.Minute))
	elapsed := time.Since(start)

	// Without forwarding, the connector's own WithInsufficientPeersBudget(0)
	// wins, the call falls back to a single replica and succeeds at once.
	if err == nil {
		t.Fatalf("caller's insufficient-peers budget was not forwarded: call succeeded in %v", elapsed)
	}
	if elapsed < time.Second {
		t.Fatalf("caller's backoff was not forwarded: gave up after %v", elapsed)
	}
}
