package nats_connector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// MinLockTTL is the minimum lock lease duration accepted by the NATS
// server's per-message TTL implementation. Anything shorter is rejected
// with "invalid per-message TTL".
const MinLockTTL = time.Second

// bucketEnsureTimeout caps how long we wait when lazily creating the lock
// KV bucket. If NATS is unreachable, NewLock / Once fail with an error
// instead of blocking forever.
const bucketEnsureTimeout = 10 * time.Second

// ErrLockLost is returned by Unlock when the local view of the lock no
// longer matches the bucket (e.g. heartbeat failed and the lock was taken
// over by another holder).
var ErrLockLost = errors.New("nats_connector: distributed lock lost")

// LockConfig configures a Lock.
type LockConfig struct {
	// Key identifies the lock. Locks with the same Key (within the same
	// bucket) are mutually exclusive. Required.
	Key string

	// Bucket overrides the connector's default lock bucket. Optional.
	Bucket string

	// TTL is the lock lease duration. If the holder fails to heartbeat
	// within TTL, the bucket auto-expires the key and another caller can
	// acquire the lock. Defaults to the connector's lock.defaultTTL.
	TTL time.Duration

	// HeartbeatInterval is how often the lock holder renews its lease.
	// Should be safely below TTL (the default is TTL/3).
	HeartbeatInterval time.Duration

	// OwnerID identifies the holder. Defaults to "<hostname>-<random>".
	OwnerID string

	// OnLost is invoked if the heartbeat goroutine determines the lock
	// has been lost (e.g. CAS Update failed because the key was deleted
	// or taken over). Optional.
	OnLost func(error)
}

// Lock is a distributed mutex backed by a NATS JetStream KV bucket.
//
// A Lock is single-use: after Unlock returns, allocate a new Lock if you
// need to re-acquire. Concurrent use of a single Lock instance from
// multiple goroutines is NOT supported; create one Lock per goroutine
// that wants to hold the lock.
type Lock struct {
	js      jetstream.JetStream
	kv      jetstream.KeyValue
	bucket  string
	ttl     time.Duration
	hbEvery time.Duration
	ownerID string
	key     string
	payload []byte
	logger  *zap.Logger
	onLost  func(error)

	mu       sync.Mutex
	held     bool   // currently holds the lock
	lost     bool   // heartbeat detected loss; Unlock should report ErrLockLost
	used     bool   // ever successfully acquired (one-shot guard)
	lastRev  uint64
	hbCancel context.CancelFunc
	hbDone   chan struct{}

	doneCh     chan struct{}
	doneClosed sync.Once
}

func newLock(
	js jetstream.JetStream,
	defaultBucket string,
	defaultTTL time.Duration,
	replicas int,
	logger *zap.Logger,
	cfg LockConfig,
) (*Lock, error) {

	if cfg.Key == "" {
		return nil, errors.New("nats_connector: LockConfig.Key is required")
	}

	bucket := cfg.Bucket
	if bucket == "" {
		bucket = defaultBucket
	}

	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultTTL
	}
	if ttl < MinLockTTL {
		return nil, fmt.Errorf("nats_connector: LockConfig.TTL %s is below the NATS per-message TTL minimum (%s)", ttl, MinLockTTL)
	}

	hbEvery := cfg.HeartbeatInterval
	if hbEvery <= 0 {
		hbEvery = ttl / 3
		if hbEvery <= 0 {
			hbEvery = time.Second
		}
	}

	ownerID := cfg.OwnerID
	if ownerID == "" {
		ownerID = defaultOwnerID()
	}

	ensureCtx, cancel := context.WithTimeout(context.Background(), bucketEnsureTimeout)
	defer cancel()
	kv, err := ensureLockBucket(ensureCtx, js, bucket, replicas)
	if err != nil {
		return nil, fmt.Errorf("ensure lock bucket %q: %w", bucket, err)
	}

	return newLockWithKV(js, kv, bucket, ttl, hbEvery, ownerID, cfg.Key, logger, cfg.OnLost), nil
}

// newLockWithKV is the bucket-injected constructor used by runOnce to
// avoid re-creating the KV handle.
func newLockWithKV(
	js jetstream.JetStream,
	kv jetstream.KeyValue,
	bucket string,
	ttl time.Duration,
	hbEvery time.Duration,
	ownerID string,
	key string,
	logger *zap.Logger,
	onLost func(error),
) *Lock {
	return &Lock{
		js:      js,
		kv:      kv,
		bucket:  bucket,
		ttl:     ttl,
		hbEvery: hbEvery,
		ownerID: ownerID,
		key:     lockKey(key),
		payload: []byte(ownerID),
		logger:  logger,
		onLost:  onLost,
		doneCh:  make(chan struct{}),
	}
}

// Lock blocks until the lock is acquired, ctx is cancelled, or an
// unrecoverable error occurs.
func (l *Lock) Lock(ctx context.Context) error {
	for {
		ok, err := l.TryLock(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}

		if err := l.waitForRelease(ctx); err != nil {
			return err
		}
	}
}

// TryLock attempts to acquire the lock once. Returns (true, nil) if
// acquired, (false, nil) if the lock is currently held by someone else,
// or (false, err) on infrastructure error.
//
// Each Lock instance may only be acquired once. After Unlock returns,
// any further Lock/TryLock calls on the same instance return an error.
func (l *Lock) TryLock(ctx context.Context) (bool, error) {
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return false, errors.New("nats_connector: lock already held by this instance")
	}
	if l.used {
		l.mu.Unlock()
		return false, errors.New("nats_connector: Lock is single-use; allocate a new Lock to re-acquire")
	}
	l.mu.Unlock()

	rev, err := l.kv.Create(ctx, l.key, l.payload, jetstream.KeyTTL(l.ttl))
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return false, nil
		}
		return false, fmt.Errorf("kv create: %w", err)
	}

	l.mu.Lock()
	l.held = true
	l.used = true
	l.lastRev = rev
	l.mu.Unlock()

	l.startHeartbeat()
	return true, nil
}

// Unlock releases the lock. Idempotent: calling Unlock more than once,
// or before Lock succeeds, is a no-op.
//
// Returns ErrLockLost if the heartbeat goroutine determined the lock had
// already been lost (another holder may have taken over). The returned
// error always wraps ErrLockLost in that case, so callers can use
// errors.Is(err, ErrLockLost).
func (l *Lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	if !l.held && !l.lost {
		l.mu.Unlock()
		return nil
	}
	l.held = false
	l.mu.Unlock()

	// Stop and DRAIN the heartbeat goroutine before deciding what to do.
	// If the heartbeat was mid-flight when we entered Unlock, it may set
	// l.lost in handleLost; we must observe that final state.
	l.stopHeartbeat()
	defer l.markDone()

	l.mu.Lock()
	lost := l.lost
	rev := l.lastRev
	l.mu.Unlock()

	if lost {
		return ErrLockLost
	}

	if err := l.kv.Delete(ctx, l.key, jetstream.LastRevision(rev)); err != nil {
		// Wrong revision typically means another holder has taken over
		// (e.g. the lease expired and someone else acquired). Surface
		// this as ErrLockLost so callers can branch on it.
		return fmt.Errorf("%w: kv delete: %v", ErrLockLost, err)
	}
	return nil
}

// Done returns a channel that is closed when the lock is released, either
// by Unlock or because the heartbeat goroutine detected a loss.
func (l *Lock) Done() <-chan struct{} {
	return l.doneCh
}

// OwnerID returns the identity used to claim this lock.
func (l *Lock) OwnerID() string {
	return l.ownerID
}

// Bucket returns the KV bucket the lock lives in.
func (l *Lock) Bucket() string {
	return l.bucket
}

func (l *Lock) waitForRelease(ctx context.Context) error {
	// Watch with the default options: the watcher emits the current
	// value (if any) as an "initial" entry, then a nil sentinel marking
	// end-of-initial, then future updates.
	watcher, err := l.kv.Watch(ctx, l.key)
	if err != nil {
		return l.pollUntilGone(ctx)
	}
	defer watcher.Stop()

	var initialDone, keyPresent bool
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case entry, ok := <-watcher.Updates():
			if !ok {
				return nil
			}
			if entry == nil {
				initialDone = true
				if !keyPresent {
					// Key already gone at watch time; outer loop
					// should re-try Create immediately.
					return nil
				}
				continue
			}
			switch entry.Operation() {
			case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
				return nil
			default:
				if !initialDone {
					keyPresent = true
				}
				// A Put / Create means someone else holds it; keep waiting.
			}

		case <-time.After(l.ttl + time.Second):
			// Safety net: if a delete event was missed (rare), bail
			// out — the outer Lock loop re-tries Create which is the
			// authoritative check.
			return nil
		}
	}
}

func (l *Lock) pollUntilGone(ctx context.Context) error {
	interval := l.hbEvery
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_, err := l.kv.Get(ctx, l.key)
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				return nil
			}
			// Any other state — keep polling.
		}
	}
}

func (l *Lock) startHeartbeat() {
	ctx, cancel := context.WithCancel(context.Background())

	l.mu.Lock()
	l.hbCancel = cancel
	l.hbDone = make(chan struct{})
	hbDone := l.hbDone
	l.mu.Unlock()

	go func() {
		defer close(hbDone)
		t := time.NewTicker(l.hbEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !l.beat(ctx) {
					return
				}
			}
		}
	}()
}

func (l *Lock) stopHeartbeat() {
	l.mu.Lock()
	cancel := l.hbCancel
	done := l.hbDone
	l.hbCancel = nil
	l.hbDone = nil
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// beat performs one heartbeat. Returns false if the lock is lost and the
// heartbeat loop should exit.
func (l *Lock) beat(ctx context.Context) bool {
	l.mu.Lock()
	rev := l.lastRev
	l.mu.Unlock()

	// Update the value with the latest revision; this also resets the
	// per-key TTL on the bucket.
	newRev, err := l.kv.Update(ctx, l.key, l.payload, rev)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return false
		}
		// Wrong revision or key deleted → we lost the lock.
		l.handleLost(fmt.Errorf("heartbeat update: %w", err))
		return false
	}

	l.mu.Lock()
	l.lastRev = newRev
	l.mu.Unlock()
	return true
}

func (l *Lock) handleLost(cause error) {
	l.mu.Lock()
	l.held = false
	l.lost = true
	l.mu.Unlock()

	l.markDone()
	if l.onLost != nil {
		l.onLost(cause)
	} else if l.logger != nil {
		l.logger.Warn("distributed lock lost",
			zap.String("bucket", l.bucket),
			zap.String("key", l.key),
			zap.Error(cause),
		)
	}
}

func (l *Lock) markDone() {
	l.doneClosed.Do(func() {
		close(l.doneCh)
	})
}

// ---- bucket / key helpers ----

func lockKey(name string) string {
	return "lock." + name
}

func doneKey(name string) string {
	return "done." + name
}

func defaultOwnerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return host + "-" + hex.EncodeToString(b[:])
}

func ensureLockBucket(
	ctx context.Context,
	js jetstream.JetStream,
	bucket string,
	replicas int,
) (jetstream.KeyValue, error) {
	if replicas <= 0 {
		replicas = 1
	}
	cfg := jetstream.KeyValueConfig{
		Bucket:         bucket,
		Description:    "Distributed locks managed by nats_connector",
		History:        1,
		Replicas:       replicas,
		LimitMarkerTTL: time.Hour, // enable per-key TTL on this bucket
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, cfg)
	if err != nil && replicas > 1 && isReplicasUnsupportedErr(err) {
		// Cluster is too small (or absent) to honour the requested
		// replica count. Fall back to a single replica so single-mode
		// servers and small clusters keep working.
		cfg.Replicas = 1
		return js.CreateOrUpdateKeyValue(ctx, cfg)
	}
	return kv, err
}

// isReplicasUnsupportedErr returns true if err looks like JetStream
// rejecting the requested replica count (e.g. non-clustered server or
// cluster smaller than Replicas).
func isReplicasUnsupportedErr(err error) bool {
	if err == nil {
		return false
	}
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) && apiErr.Description != "" {
		if matchesReplicaUnsupportedMsg(apiErr.Description) {
			return true
		}
	}
	return matchesReplicaUnsupportedMsg(err.Error())
}

func matchesReplicaUnsupportedMsg(msg string) bool {
	m := strings.ToLower(msg)
	if !strings.Contains(m, "replica") &&
		!strings.Contains(m, "insufficient") &&
		!strings.Contains(m, "peers") &&
		!strings.Contains(m, "cluster") {
		return false
	}
	return strings.Contains(m, "not supported") ||
		strings.Contains(m, "insufficient") ||
		strings.Contains(m, "no suitable peers") ||
		strings.Contains(m, "non-clustered") ||
		strings.Contains(m, "not in clustered mode") ||
		strings.Contains(m, "required") ||
		strings.Contains(m, "not available")
}

// ---- Once ----

// runOnce implements NATSConnector.Once. See the docstring there.
func runOnce(
	ctx context.Context,
	js jetstream.JetStream,
	defaultBucket string,
	defaultTTL time.Duration,
	replicas int,
	logger *zap.Logger,
	key string,
	fn func(context.Context) error,
) error {

	if key == "" {
		return errors.New("nats_connector: Once key is required")
	}
	if fn == nil {
		return errors.New("nats_connector: Once fn is required")
	}

	ensureCtx, cancel := context.WithTimeout(ctx, bucketEnsureTimeout)
	kv, err := ensureLockBucket(ensureCtx, js, defaultBucket, replicas)
	cancel()
	if err != nil {
		return fmt.Errorf("ensure lock bucket: %w", err)
	}

	// Fast path: done sentinel already exists.
	if _, err := kv.Get(ctx, doneKey(key)); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("kv get done sentinel: %w", err)
	}

	ttl := max(defaultTTL, MinLockTTL)
	hbEvery := ttl / 3
	if hbEvery <= 0 {
		hbEvery = time.Second
	}

	lock := newLockWithKV(js, kv, defaultBucket, ttl, hbEvery, defaultOwnerID(), key, logger, nil)

	if err := lock.Lock(ctx); err != nil {
		return fmt.Errorf("acquire once lock: %w", err)
	}
	defer func() {
		// Release on a fresh context so we still unlock even if the
		// caller's ctx was cancelled mid-fn.
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer releaseCancel()
		_ = lock.Unlock(releaseCtx)
	}()

	// Re-check under the lock: another instance might have just finished.
	if _, err := kv.Get(ctx, doneKey(key)); err == nil {
		return nil
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return fmt.Errorf("kv get done sentinel (under lock): %w", err)
	}

	if err := fn(ctx); err != nil {
		return err
	}

	stamp := lock.OwnerID() + "@" + time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := kv.PutString(ctx, doneKey(key), stamp); err != nil {
		return fmt.Errorf("mark once done: %w", err)
	}
	return nil
}
