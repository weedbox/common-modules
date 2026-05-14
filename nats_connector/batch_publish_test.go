package nats_connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func newBatchConnector(t *testing.T, r *testRig) *NATSConnector {
	t.Helper()
	js1, err := r.nc.JetStream()
	if err != nil {
		t.Fatalf("legacy JetStream(): %v", err)
	}
	return &NATSConnector{
		logger: zap.NewNop(),
		conn:   r.nc,
		js:     js1,
		jsv2:   r.js,
		scope:  "batchtest",
	}
}

func ensureStream(t *testing.T, r *testRig, name string, subjects []string, allowAtomic bool) {
	t.Helper()
	ctx := context.Background()
	_, err := r.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:               name,
		Subjects:           subjects,
		Storage:            jetstream.MemoryStorage,
		AllowAtomicPublish: allowAtomic,
	})
	if err != nil {
		t.Fatalf("create stream %q: %v", name, err)
	}
}

func makeItems(subject string, n int) []BatchPublishItem {
	items := make([]BatchPublishItem, n)
	for i := range n {
		items[i] = BatchPublishItem{
			Subject: subject,
			Data:    fmt.Appendf(nil, "payload-%d", i+1),
		}
	}
	return items
}

// --- BatchPublish (async fan-out, cross-stream) -----------------------------

func TestBatchPublish_BasicFanOut(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "BP1", []string{"bp1.>"}, false)
	c := newBatchConnector(t, r)

	res, err := c.BatchPublish(context.Background(), makeItems("bp1.x", 4))
	if err != nil {
		t.Fatalf("BatchPublish: %v", err)
	}
	if res.Count != 4 || len(res.Acks) != 4 {
		t.Fatalf("count/acks want 4, got %d/%d", res.Count, len(res.Acks))
	}
	for i, ack := range res.Acks {
		if ack == nil || ack.Stream != "BP1" {
			t.Errorf("ack %d unexpected: %+v", i, ack)
		}
	}
}

func TestBatchPublish_AcrossStreams(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "BP_A", []string{"bp_a.>"}, false)
	ensureStream(t, r, "BP_B", []string{"bp_b.>"}, false)
	c := newBatchConnector(t, r)

	items := []BatchPublishItem{
		{Subject: "bp_a.one", Data: []byte("a1")},
		{Subject: "bp_b.one", Data: []byte("b1")},
		{Subject: "bp_a.two", Data: []byte("a2")},
		{Subject: "bp_b.two", Data: []byte("b2")},
	}
	res, err := c.BatchPublish(context.Background(), items)
	if err != nil {
		t.Fatalf("BatchPublish: %v", err)
	}
	wantStreams := []string{"BP_A", "BP_B", "BP_A", "BP_B"}
	for i, ack := range res.Acks {
		if ack.Stream != wantStreams[i] {
			t.Errorf("ack %d stream want %s, got %s", i, wantStreams[i], ack.Stream)
		}
	}
}

func TestBatchPublish_EmptyBatchErrors(t *testing.T) {
	r := newRig(t)
	c := newBatchConnector(t, r)
	if _, err := c.BatchPublish(context.Background(), nil); err == nil {
		t.Fatal("expected error for empty batch")
	}
}

func TestBatchPublish_EmptySubjectErrors(t *testing.T) {
	r := newRig(t)
	c := newBatchConnector(t, r)
	items := []BatchPublishItem{{Subject: "ok", Data: []byte("a")}, {Subject: "", Data: []byte("b")}}
	if _, err := c.BatchPublish(context.Background(), items); err == nil {
		t.Fatal("expected error for empty subject")
	}
}

func TestBatchPublish_HeadersPreserved(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "BP_H", []string{"bp_h.>"}, false)
	c := newBatchConnector(t, r)

	items := []BatchPublishItem{{
		Subject: "bp_h.x",
		Data:    []byte("hello"),
		Header:  nats.Header{"X-Custom": []string{"value-1"}},
	}}
	if _, err := c.BatchPublish(context.Background(), items); err != nil {
		t.Fatalf("BatchPublish: %v", err)
	}

	ctx := context.Background()
	stream, err := r.js.Stream(ctx, "BP_H")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	rsm, err := stream.GetMsg(ctx, 1)
	if err != nil {
		t.Fatalf("GetMsg: %v", err)
	}
	if got := rsm.Header.Get("X-Custom"); got != "value-1" {
		t.Errorf("X-Custom want value-1, got %q", got)
	}
}

func TestBatchPublish_ContextCancel(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "BP_C", []string{"bp_c.>"}, false)
	c := newBatchConnector(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.BatchPublish(ctx, makeItems("bp_c.x", 50))
	if err == nil {
		t.Fatal("expected error after context cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Logf("note: error does not wrap context.Canceled directly: %v", err)
	}
}

// --- AtomicPublish (single stream, atomic-or-fallback) ----------------------

func TestAtomicPublish_AtomicWhenSupported(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP1", []string{"ap1.>"}, true)
	c := newBatchConnector(t, r)

	res, err := c.AtomicPublish(context.Background(), makeItems("ap1.x", 5))
	if err != nil {
		t.Fatalf("AtomicPublish: %v", err)
	}
	if res.Mode != AtomicPublishModeAtomic {
		t.Errorf("mode want atomic, got %q", res.Mode)
	}
	if res.Stream != "AP1" || res.Count != 5 {
		t.Errorf("stream/count want AP1/5, got %s/%d", res.Stream, res.Count)
	}
	if res.BatchID == "" {
		t.Error("BatchID empty")
	}
	if res.FirstSeq != 1 || res.LastSeq != 5 {
		t.Errorf("seq window want [1,5], got [%d,%d]", res.FirstSeq, res.LastSeq)
	}
	if len(res.Acks) != 0 {
		t.Errorf("atomic mode should not populate per-item Acks, got %d", len(res.Acks))
	}
}

func TestAtomicPublish_AsyncFallbackWhenStreamLacksSupport(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_FB", []string{"ap_fb.>"}, false)
	c := newBatchConnector(t, r)

	res, err := c.AtomicPublish(context.Background(), makeItems("ap_fb.x", 3))
	if err != nil {
		t.Fatalf("AtomicPublish: %v", err)
	}
	if res.Mode != AtomicPublishModeAsyncFallback {
		t.Errorf("mode want async-fallback, got %q", res.Mode)
	}
	if res.Stream != "AP_FB" {
		t.Errorf("stream want AP_FB, got %q", res.Stream)
	}
	if len(res.Acks) != 3 {
		t.Fatalf("acks want 3, got %d", len(res.Acks))
	}
	if res.FirstSeq != 1 || res.LastSeq != 3 {
		t.Errorf("seq window want [1,3], got [%d,%d]", res.FirstSeq, res.LastSeq)
	}
	if res.BatchID != "" {
		t.Errorf("BatchID should be empty in fallback, got %q", res.BatchID)
	}
}

func TestAtomicPublish_StrictRefusesFallback(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_S", []string{"ap_s.>"}, false)
	c := newBatchConnector(t, r)

	_, err := c.AtomicPublish(context.Background(), makeItems("ap_s.x", 2), WithStrictAtomic())
	if err == nil {
		t.Fatal("expected error in strict mode against non-atomic stream")
	}
	if !strings.Contains(err.Error(), "atomic") {
		t.Errorf("error should mention atomic support, got: %v", err)
	}
}

func TestAtomicPublish_CustomBatchID(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_ID", []string{"ap_id.>"}, true)
	c := newBatchConnector(t, r)

	const id = "my-custom-batch"
	res, err := c.AtomicPublish(context.Background(), makeItems("ap_id.x", 2),
		WithAtomicStream("AP_ID"), WithAtomicID(id))
	if err != nil {
		t.Fatalf("AtomicPublish: %v", err)
	}
	if res.BatchID != id {
		t.Errorf("BatchID want %q, got %q", id, res.BatchID)
	}
}

func TestAtomicPublish_SingleItem(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_ONE", []string{"ap_one.>"}, true)
	c := newBatchConnector(t, r)

	res, err := c.AtomicPublish(context.Background(), makeItems("ap_one.x", 1),
		WithAtomicStream("AP_ONE"))
	if err != nil {
		t.Fatalf("AtomicPublish: %v", err)
	}
	if res.FirstSeq != 1 || res.LastSeq != 1 {
		t.Errorf("seq window want [1,1], got [%d,%d]", res.FirstSeq, res.LastSeq)
	}
}

func TestAtomicPublish_ExplicitStreamHintSkipsLookup(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_HINT", []string{"ap_hint.>"}, true)
	c := newBatchConnector(t, r)

	res, err := c.AtomicPublish(context.Background(), makeItems("ap_hint.x", 2),
		WithAtomicStream("AP_HINT"))
	if err != nil {
		t.Fatalf("AtomicPublish: %v", err)
	}
	if res.Mode != AtomicPublishModeAtomic {
		t.Errorf("mode want atomic, got %q", res.Mode)
	}
}

func TestAtomicPublish_EmptyBatchErrors(t *testing.T) {
	r := newRig(t)
	c := newBatchConnector(t, r)
	if _, err := c.AtomicPublish(context.Background(), nil); err == nil {
		t.Fatal("expected error for empty batch")
	}
}

func TestAtomicPublish_ContextCancelInterruptsIntermediate(t *testing.T) {
	r := newRig(t)
	ensureStream(t, r, "AP_CC", []string{"ap_cc.>"}, true)
	c := newBatchConnector(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.AtomicPublish(ctx, makeItems("ap_cc.x", 200), WithAtomicStream("AP_CC"))
	if err == nil {
		t.Fatal("expected error after context cancel")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error should wrap context.Canceled, got: %v", err)
	}
}

func TestAtomicPublish_NoStreamBindingFallsBackToAsync(t *testing.T) {
	// Subject is not bound to any stream — StreamNameBySubject fails.
	// Without WithStrictAtomic we expect fallback to async, which will
	// then fail to publish because there is no listener (no stream).
	r := newRig(t)
	c := newBatchConnector(t, r)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Without a stream binding the fallback PublishMsgAsync will hit
	// ErrNoStreamResponse — surface as an error.
	_, err := c.AtomicPublish(ctx, makeItems("nowhere.x", 2))
	if err == nil {
		t.Fatal("expected error when no stream is bound")
	}
}
