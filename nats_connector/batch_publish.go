package nats_connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// AtomicPublishMode reports which path AtomicPublish actually took.
type AtomicPublishMode string

const (
	// AtomicPublishModeAtomic means the call used JetStream Atomic Batch
	// Publish (NATS 2.12+, stream must advertise AllowAtomicPublish).
	AtomicPublishModeAtomic AtomicPublishMode = "atomic"

	// AtomicPublishModeAsyncFallback means the target stream didn't
	// support atomic batch publish (or the server was too old) so the
	// call fell back to PublishMsgAsync fan-out.
	AtomicPublishModeAsyncFallback AtomicPublishMode = "async-fallback"
)

// Atomic batch protocol headers, as defined by nats-server.
const (
	batchHeaderID       = "Nats-Batch-Id"
	batchHeaderSequence = "Nats-Batch-Sequence"
	batchHeaderCommit   = "Nats-Batch-Commit"

	// DefaultBatchPublishTimeout caps the round-trip wait when the
	// caller's context has no deadline.
	DefaultBatchPublishTimeout = 30 * time.Second
)

// BatchPublishItem is one message in a batch. Subject is required; Data
// and Header are optional.
type BatchPublishItem struct {
	Subject string
	Data    []byte
	Header  nats.Header
}

// BatchPublishResult summarises BatchPublish (async fan-out, cross-stream
// friendly).
type BatchPublishResult struct {
	// Count is the number of items published.
	Count int

	// Acks holds the per-item acknowledgement, in the same order as the
	// input items. Each ack carries its own Stream, so callers can tell
	// which stream a given item landed in when subjects span multiple
	// streams.
	Acks []*jetstream.PubAck
}

// AtomicPublishResult summarises AtomicPublish, which targets a single
// stream and prefers JetStream Atomic Batch Publish.
type AtomicPublishResult struct {
	// Mode reports which path was used.
	Mode AtomicPublishMode

	// Count is the number of items published.
	Count int

	// Stream is the target stream name.
	Stream string

	// BatchID is the Nats-Batch-Id used in atomic mode. Empty when the
	// call fell back to async.
	BatchID string

	// FirstSeq / LastSeq are the assigned stream sequence numbers. In
	// atomic mode the server returns only the last sequence; FirstSeq is
	// derived as LastSeq - Count + 1. In async-fallback mode they
	// reflect the min/max sequence observed across the per-item acks.
	FirstSeq uint64
	LastSeq  uint64

	// Acks holds the per-item acks returned by the async fallback path,
	// in input order. Empty when Mode == AtomicPublishModeAtomic (the
	// server returns a single aggregate ack instead).
	Acks []*jetstream.PubAck
}

// --- Options ----------------------------------------------------------------

type batchPublishOpts struct {
	timeout time.Duration
}

// BatchPublishOption configures BatchPublish.
type BatchPublishOption func(*batchPublishOpts)

// WithBatchTimeout overrides DefaultBatchPublishTimeout. Only consulted
// when the caller's context has no deadline.
func WithBatchTimeout(d time.Duration) BatchPublishOption {
	return func(o *batchPublishOpts) { o.timeout = d }
}

type atomicPublishOpts struct {
	stream  string
	timeout time.Duration
	batchID string
	strict  bool
}

// AtomicPublishOption configures AtomicPublish.
type AtomicPublishOption func(*atomicPublishOpts)

// WithAtomicStream provides the target stream name explicitly, skipping
// the StreamNameBySubject lookup. Required when the subject is bound to
// multiple streams or the binding is ambiguous.
func WithAtomicStream(name string) AtomicPublishOption {
	return func(o *atomicPublishOpts) { o.stream = name }
}

// WithAtomicTimeout overrides DefaultBatchPublishTimeout for the commit
// round-trip. Only consulted when the caller's context has no deadline.
func WithAtomicTimeout(d time.Duration) AtomicPublishOption {
	return func(o *atomicPublishOpts) { o.timeout = d }
}

// WithAtomicID overrides the auto-generated Nats-Batch-Id.
func WithAtomicID(id string) AtomicPublishOption {
	return func(o *atomicPublishOpts) { o.batchID = id }
}

// WithStrictAtomic disables the async fallback: if the target stream
// does not support atomic batch publish, AtomicPublish returns an error
// instead of fanning out.
func WithStrictAtomic() AtomicPublishOption {
	return func(o *atomicPublishOpts) { o.strict = true }
}

// --- BatchPublish -----------------------------------------------------------

// BatchPublish fans a slice of messages out via PublishMsgAsync and waits
// for all acks. Items may target subjects bound to different streams.
// There is no all-or-nothing guarantee; if any item fails the call
// returns the first error and the remaining acks are abandoned.
// Messages that were already flushed to the server prior to the error
// may still land — the server is the source of truth. Use AtomicPublish
// when you need transactional semantics over a single stream.
func (c *NATSConnector) BatchPublish(
	ctx context.Context,
	items []BatchPublishItem,
	opts ...BatchPublishOption,
) (*BatchPublishResult, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	if err := validateItems(items); err != nil {
		return nil, err
	}

	o := batchPublishOpts{timeout: DefaultBatchPublishTimeout}
	for _, opt := range opts {
		opt(&o)
	}

	acks, err := c.fanOutAsync(ctx, items, o.timeout)
	if err != nil {
		return nil, err
	}
	return &BatchPublishResult{Count: len(items), Acks: acks}, nil
}

// --- AtomicPublish ----------------------------------------------------------

// AtomicPublish publishes a batch of messages targeting a single stream,
// preferring JetStream Atomic Batch Publish (all-or-nothing). When the
// target stream does not advertise AllowAtomicPublish (or the server is
// too old) the call transparently falls back to async fan-out unless
// WithStrictAtomic is set.
//
// All items are expected to land in the same stream. In atomic mode the
// server enforces this; in async-fallback mode the connector does not
// pre-check it, so callers should target subjects bound to one stream.
//
// Failure modes worth knowing about:
//
//   - Errors during intermediate publishes leave a partial batch on the
//     server. The server discards it once its atomic-batch staging
//     timeout elapses; you can safely retry with a fresh batch.
//   - If the commit RequestMsg times out or the connection drops after
//     the commit lands, the server may still apply the batch but the
//     caller will observe an error. The outcome is undetermined unless
//     you correlate via Nats-Msg-Id dedup or Nats-Batch-Id (the latter
//     is not durable past the batch's own lifetime).
//   - Reusing the same WithAtomicID across two AtomicPublish calls is
//     not supported; the server may treat the second call as a
//     continuation of the first.
func (c *NATSConnector) AtomicPublish(
	ctx context.Context,
	items []BatchPublishItem,
	opts ...AtomicPublishOption,
) (*AtomicPublishResult, error) {
	if err := c.checkReady(); err != nil {
		return nil, err
	}
	if err := validateItems(items); err != nil {
		return nil, err
	}

	o := atomicPublishOpts{timeout: DefaultBatchPublishTimeout}
	for _, opt := range opts {
		opt(&o)
	}

	canAtomic, stream := c.streamSupportsAtomic(ctx, items[0].Subject, o.stream)
	if !canAtomic {
		if o.strict {
			return nil, fmt.Errorf("nats_connector: AtomicPublish: stream %q does not support atomic batch publish", stream)
		}
		acks, err := c.fanOutAsync(ctx, items, o.timeout)
		if err != nil {
			return nil, err
		}
		res := &AtomicPublishResult{
			Mode:   AtomicPublishModeAsyncFallback,
			Count:  len(items),
			Stream: stream,
			Acks:   acks,
		}
		firstSet := false
		for _, ack := range acks {
			if ack == nil {
				continue
			}
			if res.Stream == "" {
				res.Stream = ack.Stream
			}
			if !firstSet || ack.Sequence < res.FirstSeq {
				res.FirstSeq = ack.Sequence
				firstSet = true
			}
			if ack.Sequence > res.LastSeq {
				res.LastSeq = ack.Sequence
			}
		}
		return res, nil
	}

	return c.publishAtomic(ctx, items, stream, o)
}

// --- internals --------------------------------------------------------------

func (c *NATSConnector) checkReady() error {
	if c.conn == nil {
		return errors.New("nats_connector: connection not initialized")
	}
	if c.jsv2 == nil {
		return errors.New("nats_connector: jetstream not initialized")
	}
	return nil
}

func validateItems(items []BatchPublishItem) error {
	if len(items) == 0 {
		return errors.New("nats_connector: batch requires at least one item")
	}
	for i, it := range items {
		if it.Subject == "" {
			return fmt.Errorf("nats_connector: batch item %d has empty Subject", i)
		}
	}
	return nil
}

// streamSupportsAtomic reports whether the target stream supports
// AllowAtomicPublish, and returns the resolved stream name (which may be
// empty when no binding could be found and the caller did not provide a
// hint).
func (c *NATSConnector) streamSupportsAtomic(ctx context.Context, subject, hint string) (bool, string) {
	stream := hint
	if stream == "" {
		name, err := c.jsv2.StreamNameBySubject(ctx, subject)
		if err != nil {
			return false, ""
		}
		stream = name
	}

	info, err := c.jsv2.Stream(ctx, stream)
	if err != nil {
		return false, stream
	}
	cfg, err := info.Info(ctx)
	if err != nil {
		return false, stream
	}
	return cfg.Config.AllowAtomicPublish, stream
}

func (c *NATSConnector) publishAtomic(
	ctx context.Context,
	items []BatchPublishItem,
	stream string,
	o atomicPublishOpts,
) (*AtomicPublishResult, error) {
	batchID := o.batchID
	if batchID == "" {
		batchID = newBatchID()
	}

	for i := 0; i < len(items)-1; i++ {
		// Bail out before flushing more messages if the caller cancelled.
		// Intermediate messages are fire-and-forget; the partial batch
		// already buffered on the server will be discarded once its
		// atomic-batch timeout elapses.
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("atomic publish item %d: %w", i, err)
		}
		m := buildBatchMsg(items[i], batchID, uint64(i+1), false)
		if err := c.conn.PublishMsg(m); err != nil {
			return nil, fmt.Errorf("atomic publish item %d: %w", i, err)
		}
	}

	commit := buildBatchMsg(items[len(items)-1], batchID, uint64(len(items)), true)

	reqCtx, cancel := contextWithTimeout(ctx, o.timeout)
	defer cancel()

	rmsg, err := c.conn.RequestMsgWithContext(reqCtx, commit)
	if err != nil {
		return nil, fmt.Errorf("atomic commit: %w", err)
	}
	if len(rmsg.Data) == 0 {
		return nil, errors.New("nats_connector: atomic commit returned empty ack")
	}

	var ack atomicBatchAck
	if err := json.Unmarshal(rmsg.Data, &ack); err != nil {
		return nil, fmt.Errorf("decode atomic ack: %w", err)
	}
	if ack.Error != nil {
		return nil, fmt.Errorf("atomic batch rejected: %s (code=%d, err_code=%d)", ack.Error.Description, ack.Error.Code, ack.Error.ErrCode)
	}

	res := &AtomicPublishResult{
		Mode:    AtomicPublishModeAtomic,
		Count:   len(items),
		Stream:  ack.Stream,
		BatchID: ack.BatchID,
		LastSeq: ack.Sequence,
	}
	if res.Stream == "" {
		res.Stream = stream
	}
	if res.BatchID == "" {
		res.BatchID = batchID
	}
	if ack.BatchSize > 0 && uint64(ack.BatchSize) <= ack.Sequence {
		res.FirstSeq = ack.Sequence - uint64(ack.BatchSize) + 1
	} else if res.LastSeq >= uint64(len(items)) {
		res.FirstSeq = res.LastSeq - uint64(len(items)) + 1
	}
	return res, nil
}

func (c *NATSConnector) fanOutAsync(
	ctx context.Context,
	items []BatchPublishItem,
	timeout time.Duration,
) ([]*jetstream.PubAck, error) {
	futures := make([]jetstream.PubAckFuture, 0, len(items))
	for i, it := range items {
		m := &nats.Msg{Subject: it.Subject, Data: it.Data, Header: cloneHeader(it.Header)}
		paf, err := c.jsv2.PublishMsgAsync(m)
		if err != nil {
			return nil, fmt.Errorf("async publish item %d: %w", i, err)
		}
		futures = append(futures, paf)
	}

	waitCtx, cancel := contextWithTimeout(ctx, timeout)
	defer cancel()

	acks := make([]*jetstream.PubAck, len(futures))
	for i, paf := range futures {
		select {
		case ack := <-paf.Ok():
			acks[i] = ack
		case err := <-paf.Err():
			return nil, fmt.Errorf("async publish item %d: %w", i, err)
		case <-waitCtx.Done():
			return nil, fmt.Errorf("async publish wait: %w", waitCtx.Err())
		}
	}
	return acks, nil
}

// atomicBatchAck mirrors the server's JSPubAckResponse wire format,
// which nats.go's PubAck doesn't yet expose. The server returns the last
// sequence in Sequence and the batch metadata in batch/count.
type atomicBatchAck struct {
	Error     *atomicBatchAckError `json:"error,omitempty"`
	Stream    string               `json:"stream,omitempty"`
	Sequence  uint64               `json:"seq,omitempty"`
	Duplicate bool                 `json:"duplicate,omitempty"`
	Domain    string               `json:"domain,omitempty"`
	BatchID   string               `json:"batch,omitempty"`
	BatchSize int                  `json:"count,omitempty"`
}

type atomicBatchAckError struct {
	Code        int    `json:"code"`
	ErrCode     int    `json:"err_code,omitempty"`
	Description string `json:"description,omitempty"`
}

func buildBatchMsg(item BatchPublishItem, batchID string, seq uint64, commit bool) *nats.Msg {
	h := cloneHeader(item.Header)
	if h == nil {
		h = nats.Header{}
	}
	h.Set(batchHeaderID, batchID)
	h.Set(batchHeaderSequence, strconv.FormatUint(seq, 10))
	if commit {
		h.Set(batchHeaderCommit, "1")
	}
	return &nats.Msg{Subject: item.Subject, Data: item.Data, Header: h}
}

func cloneHeader(h nats.Header) nats.Header {
	if len(h) == 0 {
		return nil
	}
	out := make(nats.Header, len(h))
	for k, v := range h {
		cp := make([]string, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func contextWithTimeout(parent context.Context, fallback time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := parent.Deadline(); ok {
		return context.WithCancel(parent)
	}
	if fallback <= 0 {
		fallback = DefaultBatchPublishTimeout
	}
	return context.WithTimeout(parent, fallback)
}

func newBatchID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "batch-" + hex.EncodeToString(b[:])
}
