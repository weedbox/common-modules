package nats_connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// Replica defaults applied when the caller does not set cfg.Replicas
// explicitly. Targets a standard 3-node JetStream cluster so newly created
// streams / KV buckets are highly available by default. On a deployment
// that cannot sustain three replicas (typically a single-node cluster
// returning err_code 10074 "insufficient resources") the convergent
// helpers transparently fall back to one replica; if the cluster later
// grows, EnsureReplicaScale opportunistically promotes the existing
// resource back toward the default.
//
// Existing streams keep whatever replica count they were originally
// created with — these constants only affect resources created (or
// scaled) through the convergent helpers.
const (
	DefaultEnsureReplicas    = 3
	EnsureFallbackReplicas   = 1
	DefaultEnsureBaseBackoff = 200 * time.Millisecond
	DefaultEnsureMaxBackoff  = 2 * time.Second
	// DefaultInsufficientPeersBudget is the window during which an
	// "insufficient peers" error is treated as a cluster-bootstrap
	// transient (per-stream Raft / peer discovery still settling) and
	// retried at the requested replica count, before falling back to a
	// single replica. Sized to comfortably cover NATS meta-leader
	// election + per-stream Raft group formation on a 3-node cluster
	// cold-start, while bounding single-node-deployment startup latency.
	DefaultInsufficientPeersBudget = 30 * time.Second
	// DefaultStuckResourceRecoveryThreshold is the recommended threshold
	// for the lookup-first stuck-recovery escape hatch (see
	// WithStuckResourceRecovery). Long enough that ordinary Raft
	// re-election (typically 1-5 s) does not trigger it, short enough
	// that a wedged resource is nudged well within a 5-minute
	// provisioning context. Applied by default in the method-form
	// helpers (*NATSConnector).EnsureStream / EnsureKV; the package-
	// level helpers leave the escape hatch disabled unless the caller
	// opts in via WithStuckResourceRecovery.
	DefaultStuckResourceRecoveryThreshold = 30 * time.Second
)

// EnsureOption configures the convergent Ensure* helpers.
type EnsureOption func(*ensureOptions)

type ensureOptions struct {
	logger                      *zap.Logger
	baseBackoff                 time.Duration
	maxBackoff                  time.Duration
	fallbackOnInsufficientPeers bool
	insufficientPeersBudget     time.Duration
	stuckRecoveryAfter          time.Duration
}

func resolveEnsureOptions(opts []EnsureOption) ensureOptions {
	o := ensureOptions{
		baseBackoff:                 DefaultEnsureBaseBackoff,
		maxBackoff:                  DefaultEnsureMaxBackoff,
		fallbackOnInsufficientPeers: true,
		insufficientPeersBudget:     DefaultInsufficientPeersBudget,
	}
	for _, fn := range opts {
		fn(&o)
	}
	if o.baseBackoff <= 0 {
		o.baseBackoff = DefaultEnsureBaseBackoff
	}
	if o.maxBackoff < o.baseBackoff {
		o.maxBackoff = o.baseBackoff
	}
	if o.insufficientPeersBudget < 0 {
		o.insufficientPeersBudget = 0
	}
	if o.stuckRecoveryAfter < 0 {
		o.stuckRecoveryAfter = 0
	}
	return o
}

// WithEnsureLogger installs a zap logger that receives structured-log
// lines on non-trivial transitions (insufficient-peers fallback, replica
// scale-up result, final scale-up failure). nil is allowed and silences
// the logger.
func WithEnsureLogger(l *zap.Logger) EnsureOption {
	return func(o *ensureOptions) { o.logger = l }
}

// WithEnsureBackoff overrides the retry backoff window. base is the
// initial sleep between retries; max caps the doubled value. Non-positive
// base resets to DefaultEnsureBaseBackoff; max below base is clamped up
// to base.
func WithEnsureBackoff(base, max time.Duration) EnsureOption {
	return func(o *ensureOptions) {
		o.baseBackoff = base
		o.maxBackoff = max
	}
}

// WithoutReplicaFallback disables the silent demotion to a single replica
// when the cluster reports insufficient peers. With this option the
// helpers surface the placement error directly instead of retrying with
// Replicas=1.
func WithoutReplicaFallback() EnsureOption {
	return func(o *ensureOptions) { o.fallbackOnInsufficientPeers = false }
}

// WithInsufficientPeersBudget overrides how long the helper treats
// "insufficient peers" as a transient cluster-bootstrap error (retrying
// at the requested replica count) before falling back to a single
// replica. Set to 0 to fall back immediately on the first occurrence —
// useful for single-node deployments that should not pay the bootstrap-
// budget wait, and for tests asserting the fallback path. Negative
// values are clamped to 0. Default DefaultInsufficientPeersBudget.
//
// Has no effect when combined with WithoutReplicaFallback (which
// disables fallback entirely).
func WithInsufficientPeersBudget(d time.Duration) EnsureOption {
	return func(o *ensureOptions) { o.insufficientPeersBudget = d }
}

// WithStuckResourceRecovery enables an escape hatch for the lookup-first
// fast path. By default, when EnsureStream / EnsureKV finds a stream or
// KV bucket that already exists but is not publishable (no leader
// elected, or some replicas offline / not current), it sleeps and
// re-checks readiness without touching the server-side definition —
// letting per-stream Raft settle on its own.
//
// That is correct for the common case (replicas catching up, transient
// leader-flap, fresh cold-start Raft formation), but it is also a blind
// spot: if a previous process or peer crash left the resource in a state
// where Raft cannot elect a leader without external nudging, the loop
// will spin until ctx expires. After a pod restart the next process
// observes the same stuck resource and stalls again — never reaching the
// recovery action the pre-lookup-first design used to perform every
// iteration.
//
// With WithStuckResourceRecovery(after), once a continuous stuck-existing
// observation has persisted for `after` duration, the helper re-issues
// CreateOrUpdateStream / CreateOrUpdateKeyValue with the requested cfg.
// CreateOrUpdate is idempotent for matching cfg and serialized by the
// meta-leader, so the call is safe to repeat under load. The recovery
// timer resets after each attempt so the helper does not spam the
// server. Errors from the recovery CreateOrUpdate are logged (when a
// logger is installed via WithEnsureLogger) but never aborted on — the
// caller is still waiting for the resource to become ready and the next
// lookup iteration may observe recovery from the nudge.
//
// Set to 0 to disable (default for the package-level helpers). Negative
// values are clamped to 0. The method-form helpers
// (*NATSConnector).EnsureStream / EnsureKV enable this by default with
// DefaultStuckResourceRecoveryThreshold; callers that need to override
// or disable it should drop down to the package-level form and pass an
// explicit WithStuckResourceRecovery(...) (including
// WithStuckResourceRecovery(0) to turn it off).
func WithStuckResourceRecovery(after time.Duration) EnsureOption {
	return func(o *ensureOptions) { o.stuckRecoveryAfter = after }
}

// EnsureKV provisions a JetStream KV bucket and blocks until the
// underlying KV_<bucket> stream is publishable, or ctx is done.
//
// Designed to be safe under multi-process cold start without external
// coordination:
//
//   - Lookup-first: every iteration starts with js.KeyValue + stream-info
//     for the underlying KV_<bucket> stream. If a leader is already
//     elected, the bucket is publishable and the call returns without
//     issuing a CreateOrUpdate. This avoids re-driving server-side Raft
//     activity on every retry when an existing bucket is mid-scale or
//     mid-catch-up, which can otherwise stall the loop for the full ctx
//     budget on streams whose new replicas can't become Current quickly.
//   - The NATS server's meta-leader serializes CreateOrUpdateKeyValue by
//     bucket name, so when the lookup misses, concurrent callers either
//     match the same config (idempotent) or one wins and the others surface
//     "bucket already exists" — both fall back to the next lookup.
//   - Transient cold-start errors (no responders, leader-flap, "no response
//     from stream", etc.) are absorbed by the retry loop.
//   - "Insufficient peers" is first treated as a cluster-bootstrap transient
//     for InsufficientPeersBudget (default 30s) — the cluster's meta-leader
//     election and per-stream Raft formation can momentarily report "no
//     suitable peers" before peer discovery completes. Only after the
//     budget elapses without recovery is cfg.Replicas demoted to 1. Once
//     the bucket is publishable EnsureReplicaScale runs a best-effort
//     promotion toward the original Replicas, which makes a cluster that
//     later grows from 1-node to 3-node self-heal on the next call.
//     Disable fallback entirely via WithoutReplicaFallback(), or shorten /
//     zero the budget via WithInsufficientPeersBudget().
//
// Config drift on an existing bucket is intentionally NOT reconciled here:
// the lookup-first short-circuits the CreateOrUpdate path when the bucket
// is publishable. Callers needing drift reconciliation should issue an
// explicit js.UpdateStream / js.CreateOrUpdateKeyValue outside this helper.
//
// ctx is the only deadline. Aborts immediately on ctx cancellation. Returns
// the resolved KeyValue handle on success.
func EnsureKV(ctx context.Context, js jetstream.JetStream, cfg jetstream.KeyValueConfig, opts ...EnsureOption) (jetstream.KeyValue, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("EnsureKV: bucket required")
	}

	o := resolveEnsureOptions(opts)

	desiredReplicas := cfg.Replicas
	if desiredReplicas <= 0 {
		desiredReplicas = DefaultEnsureReplicas
		cfg.Replicas = DefaultEnsureReplicas
	}

	backoff := o.baseBackoff
	var insufficientPeersSince time.Time
	var stuckSince time.Time
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Lookup-first fast path.
		if kv, err := js.KeyValue(ctx, cfg.Bucket); err == nil {
			if isJetStreamKVReady(ctx, js, cfg.Bucket) {
				ensureReplicaScale(ctx, js, "KV_"+cfg.Bucket, desiredReplicas, o)
				return kv, nil
			}
			// Bucket exists but underlying stream has no elected leader —
			// wait for Raft to settle instead of re-issuing CreateOrUpdate.
			if stuckSince.IsZero() {
				stuckSince = time.Now()
			}
			if o.stuckRecoveryAfter > 0 && time.Since(stuckSince) >= o.stuckRecoveryAfter {
				logEvent(o.logger, "convergent: KV bucket stuck not-ready beyond recovery threshold, re-issuing CreateOrUpdate",
					zap.String("bucket", cfg.Bucket),
					zap.Duration("stuck_for", time.Since(stuckSince)),
					zap.Duration("threshold", o.stuckRecoveryAfter))
				if _, recoverErr := js.CreateOrUpdateKeyValue(ctx, cfg); recoverErr != nil {
					// Nudge is best-effort; the original "exists but
					// stuck" condition still holds, so keep waiting and
					// let the next iteration's lookup observe whether
					// the nudge unblocked Raft.
					logEvent(o.logger, "convergent: stuck-KV recovery CreateOrUpdate returned error",
						zap.String("bucket", cfg.Bucket),
						zap.Error(recoverErr))
				}
				stuckSince = time.Now()
			}
			if err := sleepBackoff(ctx, &backoff, o.maxBackoff); err != nil {
				return nil, err
			}
			continue
		} else if !errors.Is(err, jetstream.ErrBucketNotFound) && !isTransientJetStreamError(err) {
			return nil, fmt.Errorf("lookup kv bucket %s: %w", cfg.Bucket, err)
		}
		// Either NotFound or transient — clear stuck tracking; we're
		// heading back into the create path.
		stuckSince = time.Time{}

		_, err := js.CreateOrUpdateKeyValue(ctx, cfg)
		switch {
		case err == nil:
			// Loop back so the next iteration's lookup-first path picks up
			// the handle and runs the publishable check.

		case isInsufficientPeers(err) && o.fallbackOnInsufficientPeers && cfg.Replicas > EnsureFallbackReplicas:
			firstSeen := insufficientPeersSince.IsZero()
			if firstSeen {
				insufficientPeersSince = time.Now()
			}
			if time.Since(insufficientPeersSince) < o.insufficientPeersBudget {
				if firstSeen {
					logEvent(o.logger, "convergent: KV insufficient peers within bootstrap budget, retrying at requested replicas",
						zap.String("bucket", cfg.Bucket),
						zap.Int("replicas", cfg.Replicas),
						zap.Duration("budget", o.insufficientPeersBudget),
						zap.Error(err))
				}
			} else {
				logEvent(o.logger, "convergent: KV create rejected at default replica count, falling back to single replica",
					zap.String("bucket", cfg.Bucket),
					zap.Int("attempted_replicas", cfg.Replicas),
					zap.Duration("budget_elapsed", time.Since(insufficientPeersSince)),
					zap.Error(err))
				cfg.Replicas = EnsureFallbackReplicas
			}

		case isAlreadyInUse(err) || isTransientJetStreamError(err):
			// peer is mid-create or burst-induced transient — fall through
			// to the next lookup.

		default:
			return nil, fmt.Errorf("create-or-update kv bucket %s: %w", cfg.Bucket, err)
		}

		if err := sleepBackoff(ctx, &backoff, o.maxBackoff); err != nil {
			return nil, err
		}
	}
}

// EnsureStream provisions a JetStream stream and blocks until it is
// publishable (leader elected), or ctx is done.
//
// Lookup-first: every iteration calls js.Stream to find an existing stream
// before falling through to CreateOrUpdateStream. When the stream already
// exists with an elected leader, the call returns without issuing an
// update. This avoids forcing the server to re-validate (and on Replicas
// drift, re-scale) the stream on every retry — a CreateOrUpdate-each-
// iteration loop can stall for the full ctx budget against streams whose
// scale-up to the requested replica count is gated by slow peer catch-up.
//
// Multi-process safe semantics:
//
//   - When the stream is missing, server-side meta-leader serialization by
//     name guarantees that concurrent CreateOrUpdate callers either match
//     (idempotent) or surface "stream already exists" / transient retry,
//     both of which loop back to the next lookup.
//   - "Insufficient peers" is first treated as a cluster-bootstrap transient
//     for InsufficientPeersBudget (default 30s); only after the budget
//     elapses without recovery is cfg.Replicas demoted to 1. Post-ready
//     opportunistic promotion via EnsureReplicaScale upgrades the stream
//     when the cluster later grows. Disable fallback entirely via
//     WithoutReplicaFallback(), or shorten / zero the budget via
//     WithInsufficientPeersBudget().
//
// Config drift on an existing stream is intentionally NOT reconciled here.
// Callers needing drift reconciliation should call js.UpdateStream
// explicitly outside this helper.
//
// Returns the resolved Stream handle.
func EnsureStream(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig, opts ...EnsureOption) (jetstream.Stream, error) {
	if cfg.Name == "" {
		return nil, fmt.Errorf("EnsureStream: stream name required")
	}

	o := resolveEnsureOptions(opts)

	desiredReplicas := cfg.Replicas
	if desiredReplicas <= 0 {
		desiredReplicas = DefaultEnsureReplicas
		cfg.Replicas = DefaultEnsureReplicas
	}

	backoff := o.baseBackoff
	var insufficientPeersSince time.Time
	var stuckSince time.Time
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Lookup-first fast path.
		if existing, err := js.Stream(ctx, cfg.Name); err == nil {
			if isStreamReady(ctx, existing) {
				ensureReplicaScale(ctx, js, cfg.Name, desiredReplicas, o)
				return existing, nil
			}
			// Exists, leader still settling — wait for Raft instead of
			// re-issuing CreateOrUpdate.
			if stuckSince.IsZero() {
				stuckSince = time.Now()
			}
			if o.stuckRecoveryAfter > 0 && time.Since(stuckSince) >= o.stuckRecoveryAfter {
				logEvent(o.logger, "convergent: stream stuck not-ready beyond recovery threshold, re-issuing CreateOrUpdate",
					zap.String("stream", cfg.Name),
					zap.Duration("stuck_for", time.Since(stuckSince)),
					zap.Duration("threshold", o.stuckRecoveryAfter))
				if _, recoverErr := js.CreateOrUpdateStream(ctx, cfg); recoverErr != nil {
					// Nudge is best-effort; the original "exists but
					// stuck" condition still holds, so keep waiting and
					// let the next iteration's lookup observe whether
					// the nudge unblocked Raft.
					logEvent(o.logger, "convergent: stuck-stream recovery CreateOrUpdate returned error",
						zap.String("stream", cfg.Name),
						zap.Error(recoverErr))
				}
				stuckSince = time.Now()
			}
			if err := sleepBackoff(ctx, &backoff, o.maxBackoff); err != nil {
				return nil, err
			}
			continue
		} else if !errors.Is(err, jetstream.ErrStreamNotFound) && !isTransientJetStreamError(err) {
			return nil, fmt.Errorf("lookup stream %s: %w", cfg.Name, err)
		}
		// Either NotFound or transient — clear stuck tracking; we're
		// heading back into the create path.
		stuckSince = time.Time{}

		_, err := js.CreateOrUpdateStream(ctx, cfg)
		switch {
		case err == nil:
			// Loop back so the next iteration's lookup-first path picks up
			// the handle and runs the publishable check.

		case isInsufficientPeers(err) && o.fallbackOnInsufficientPeers && cfg.Replicas > EnsureFallbackReplicas:
			firstSeen := insufficientPeersSince.IsZero()
			if firstSeen {
				insufficientPeersSince = time.Now()
			}
			if time.Since(insufficientPeersSince) < o.insufficientPeersBudget {
				if firstSeen {
					logEvent(o.logger, "convergent: stream insufficient peers within bootstrap budget, retrying at requested replicas",
						zap.String("stream", cfg.Name),
						zap.Int("replicas", cfg.Replicas),
						zap.Duration("budget", o.insufficientPeersBudget),
						zap.Error(err))
				}
			} else {
				logEvent(o.logger, "convergent: stream create rejected at default replica count, falling back to single replica",
					zap.String("stream", cfg.Name),
					zap.Int("attempted_replicas", cfg.Replicas),
					zap.Duration("budget_elapsed", time.Since(insufficientPeersSince)),
					zap.Error(err))
				cfg.Replicas = EnsureFallbackReplicas
			}

		case isAlreadyInUse(err) || isTransientJetStreamError(err):

		default:
			return nil, fmt.Errorf("create-or-update stream %s: %w", cfg.Name, err)
		}

		if err := sleepBackoff(ctx, &backoff, o.maxBackoff); err != nil {
			return nil, err
		}
	}
}

// EnsureConsumer provisions a durable consumer on the given
// stream, retrying through transient cold-start errors. Consumers inherit
// the parent stream's replica count, so there is no replica fallback here —
// the call either succeeds, surfaces a permanent config error, or loops
// against transient failures until ctx is done.
func EnsureConsumer(ctx context.Context, stream jetstream.Stream, cfg jetstream.ConsumerConfig, opts ...EnsureOption) (jetstream.Consumer, error) {
	if stream == nil {
		return nil, fmt.Errorf("EnsureConsumer: stream handle required")
	}

	o := resolveEnsureOptions(opts)

	backoff := o.baseBackoff
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		consumer, err := stream.CreateOrUpdateConsumer(ctx, cfg)
		switch {
		case err == nil:
			return consumer, nil

		case isAlreadyInUse(err) || isTransientJetStreamError(err):

		default:
			return nil, fmt.Errorf("create-or-update consumer %s: %w", cfg.Durable, err)
		}

		if err := sleepBackoff(ctx, &backoff, o.maxBackoff); err != nil {
			return nil, err
		}
	}
}

// EnsureReplicaScale opportunistically upgrades an existing stream's
// replica count toward `desired` when the cluster has grown since the
// stream was originally created (e.g., a previously single-node deployment
// scaled out to three nodes). Best-effort: any failure is logged at warn
// level (when a logger is installed via WithEnsureLogger) and swallowed
// so it cannot block the caller's startup — the next call will retry.
//
// Skips silently when: ctx already cancelled, desired ≤ 1 (nothing to scale
// to), the stream has no Cluster info (single-node server), current replica
// count already ≥ desired, or the server reports insufficient peers (the
// cluster is still too small).
//
// Multi-process safe: the server's meta-leader serializes UpdateStream by
// name and the call is idempotent for matching cfg. If two peers race the
// scale-up, the second is a no-op.
func EnsureReplicaScale(ctx context.Context, js jetstream.JetStream, streamName string, desired int, opts ...EnsureOption) {
	ensureReplicaScale(ctx, js, streamName, desired, resolveEnsureOptions(opts))
}

func ensureReplicaScale(ctx context.Context, js jetstream.JetStream, streamName string, desired int, o ensureOptions) {
	if desired <= 1 {
		return
	}
	if ctx.Err() != nil {
		return
	}
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return
	}
	info, err := stream.Info(ctx)
	if err != nil || info == nil {
		return
	}
	if info.Cluster == nil {
		return
	}
	current := 1 + len(info.Cluster.Replicas)
	if current >= desired {
		return
	}

	cfg := info.Config
	cfg.Replicas = desired
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		if isInsufficientPeers(err) || ctx.Err() != nil {
			return
		}
		logEvent(o.logger, "convergent: failed to scale stream replicas; will retry on next start",
			zap.String("stream", streamName),
			zap.Int("from", current),
			zap.Int("to", desired),
			zap.Error(err))
		return
	}
	logEvent(o.logger, "convergent: scaled stream replicas",
		zap.String("stream", streamName),
		zap.Int("from", current),
		zap.Int("to", desired))
}

// isStreamReady reports whether a JetStream stream can accept publishes
// right now. For a clustered stream this means a leader is elected AND
// every replica peer is online + caught up to the leader. For a non-
// clustered (single-node) stream a non-nil StreamInfo is sufficient.
//
// The strict "all replicas Current" criterion matches plasma's original
// pre-migration helper (pkg/event_manager/convergent.go @ 3b15498^) so
// the migration path preserves that invariant; cold-start replica catch-
// up is bounded by the cluster's Raft scheduling, not by this helper.
func isStreamReady(ctx context.Context, stream jetstream.Stream) bool {
	if stream == nil {
		return false
	}
	info, err := stream.Info(ctx)
	if err != nil || info == nil {
		return false
	}
	if info.Cluster == nil {
		return true
	}
	if info.Cluster.Leader == "" {
		return false
	}
	for _, r := range info.Cluster.Replicas {
		if r == nil || r.Offline || !r.Current {
			return false
		}
	}
	return true
}

// isJetStreamKVReady verifies the KV_<bucket> stream that backs a KV
// bucket is ready to accept writes. Mirrors isStreamReady for the KV
// lookup-first fast path.
func isJetStreamKVReady(ctx context.Context, js jetstream.JetStream, bucket string) bool {
	stream, err := js.Stream(ctx, "KV_"+bucket)
	if err != nil {
		return false
	}
	return isStreamReady(ctx, stream)
}

// isAlreadyInUse reports whether err indicates a concurrent peer has
// already created the stream / bucket / consumer with the same name. Not
// transient (the server-side state is stable) but recoverable — the
// caller's job is to fall through to a readiness verification on the
// existing resource.
func isAlreadyInUse(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "stream name already in use"),
		strings.Contains(s, "stream already exists"),
		strings.Contains(s, "bucket already exists"),
		strings.Contains(s, "consumer already exists"),
		strings.Contains(s, "name already in use"):
		return true
	}
	return false
}

// isInsufficientPeers reports whether err indicates the cluster lacks
// enough peers to satisfy the requested replica count (e.g., Replicas=3
// against a single-node deployment). err_code 10074 ("insufficient
// resources") is the canonical code; substring matches cover both the
// classic and new-API wrapped error shapes.
func isInsufficientPeers(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no suitable peers"),
		strings.Contains(s, "insufficient peers"),
		strings.Contains(s, "insufficient resources"),
		strings.Contains(s, "10074"):
		return true
	}
	return false
}

// isTransientJetStreamError reports whether err is a cold-start /
// flapping-cluster error worth retrying rather than surfacing to the
// caller.
//
// Deliberately excludes "insufficient peers" / "no suitable peers" /
// err_code 10074 — those are placement failures, handled separately by
// the replica-fallback path so the retry budget isn't burned on a state
// the cluster cannot recover from.
//
// Matches both errors.Is-able sentinels from the classic nats.* package
// (which the new jetstream v2 client still surfaces from the underlying
// nats.Conn request layer — notably nats.ErrTimeout, whose Error() does
// not match any of the substrings below) and the wrapped error strings
// the new API returns through fmt.Errorf chains.
func isTransientJetStreamError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrTimeout) {
		return true
	}
	if errors.Is(err, jetstream.ErrNoStreamResponse) {
		return true
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "no responders"),
		strings.Contains(s, "no response from stream"),
		strings.Contains(s, "context deadline exceeded"),
		strings.Contains(s, "leadership transferred"),
		strings.Contains(s, "JetStream system temporarily unavailable"),
		strings.Contains(s, "system temporarily unavailable"),
		strings.Contains(s, "stream is offline"),
		strings.Contains(s, "stream not currently available"):
		return true
	}
	return false
}

// sleepBackoff sleeps for the current backoff window, doubles it (capped
// at maxBackoff), and returns ctx.Err() if ctx is done.
func sleepBackoff(ctx context.Context, backoff *time.Duration, maxBackoff time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(*backoff):
	}
	if *backoff < maxBackoff {
		*backoff *= 2
		if *backoff > maxBackoff {
			*backoff = maxBackoff
		}
	}
	return nil
}

// logEvent emits a structured log line at info level. No-op when l is nil.
func logEvent(l *zap.Logger, msg string, fields ...zap.Field) {
	if l == nil {
		return
	}
	l.Info(msg, fields...)
}
