package nats_connector

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Message handler function type definition
type MessageHandler func(ctx context.Context, msg jetstream.Msg) error

// Batch message handler function type definition.
// Returning nil acks every message in the batch; returning an error nacks the
// entire batch (with backoff). Handlers MUST NOT call Ack/Nak/InProgress on
// individual messages — the consumer manages acknowledgement collectively.
type BatchMessageHandler func(ctx context.Context, msgs []jetstream.Msg) error

// Error handler function type definition
type ErrorHandler func(err error)

const (
	// Default values for work queue configuration
	DefaultMaxConcurrent = 10
	DefaultAckWait       = 30 * time.Second // Default ack wait time
	DefaultMaxRetries    = -1               // Default maximum retries for message processing (negative means unlimited)
	DefaultMaxAckPending = DefaultMaxConcurrent

	// Default values for restart configuration
	DefaultMaxRestarts      = -1              // Unlimited restarts
	DefaultRestartBaseDelay = 1 * time.Second
	DefaultRestartMaxDelay  = 30 * time.Second

	// Default values for batch consumption
	DefaultBatchSize    = 100
	DefaultBatchMaxWait = 1 * time.Second

	// Default values for transient-error backoff (fetch loop)
	DefaultTransientBackoffBase = 500 * time.Millisecond
	DefaultTransientBackoffMax  = 10 * time.Second
)

type WorkQueueConsumer struct {
	config   *WorkQueueConfig
	nc       *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

type WorkQueueConfig struct {
	Conn          *nats.Conn
	Stream        *nats.StreamInfo
	ConsumerName  string
	Subjects      []string
	MaxConcurrent int
	AckWait       time.Duration // Set ack wait time to prevent duplicate processing
	MaxRetries    int
	MaxAckPending    int
	OnError          ErrorHandler
	MaxRestarts      int           // Max restart attempts; -1 unlimited, 0 no restart
	RestartBaseDelay time.Duration // Restart backoff base delay (default 1s)
	RestartMaxDelay  time.Duration // Restart backoff max delay (default 30s)

	// BatchSize controls how many messages a single Fetch pulls in batch mode.
	// Only consulted by the StartBatch* APIs.
	BatchSize int
	// BatchMaxWait is the maximum time Fetch will wait to fill a batch before
	// returning whatever it has. Only consulted by the StartBatch* APIs.
	BatchMaxWait time.Duration

	// IsTransientError, if set, classifies fetch errors. Errors that match
	// are NOT reported via OnError and trigger exponential backoff in the
	// fetch loop instead of being treated as application failures. Use this
	// to swallow known transient cluster errors ("no responders", leader-
	// flap, etc.) during cold-start or network blips, so the consumer waits
	// quietly until the cluster stabilises instead of flooding logs with
	// infrastructure errors that auto-recover on the next fetch.
	//
	// When nil, all fetch errors are reported to OnError (legacy behaviour).
	IsTransientError func(error) bool

	// TransientBackoffBase / TransientBackoffMax tune the exponential
	// backoff (with jitter) applied when fetch returns a transient error.
	// Zero values fall back to DefaultTransientBackoffBase / Max.
	TransientBackoffBase time.Duration
	TransientBackoffMax  time.Duration

	// HandlerTimeout, when > 0, bounds how long a single handler invocation
	// may run. The context passed to the handler carries this deadline, so any
	// ctx-aware work it performs (database queries, outbound RPCs) is cancelled
	// once it elapses; the handler then observes ctx.Done()/ctx.Err(), unwinds,
	// and the message is nacked with backoff for retry. This guards against a
	// wedged handler holding a resource (e.g. a DB connection) indefinitely.
	//
	// When zero (the default) no deadline is applied — the handler context is
	// only cancelled on Shutdown, preserving the original behaviour. Existing
	// callers that do not set this field are unaffected.
	HandlerTimeout time.Duration
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewWorkQueueConsumerConfig() WorkQueueConfig {
	return WorkQueueConfig{
		Conn:                 nil, // Connection will be set later
		Stream:               nil, // Stream will be set later
		ConsumerName:         "default_consumer",
		Subjects:             []string{"work_queue"},
		MaxConcurrent:        DefaultMaxConcurrent,
		AckWait:              DefaultAckWait,
		MaxRetries:           DefaultMaxRetries, // Default to unlimited retries
		MaxAckPending:        DefaultMaxAckPending,
		OnError:              nil,
		MaxRestarts:          DefaultMaxRestarts,
		RestartBaseDelay:     DefaultRestartBaseDelay,
		RestartMaxDelay:      DefaultRestartMaxDelay,
		BatchSize:            DefaultBatchSize,
		BatchMaxWait:         DefaultBatchMaxWait,
		IsTransientError:     nil, // Opt-in: callers wire their own classifier
		TransientBackoffBase: DefaultTransientBackoffBase,
		TransientBackoffMax:  DefaultTransientBackoffMax,
		HandlerTimeout:       0, // Opt-in: zero means no deadline (legacy behaviour)
	}
}

func NewWorkQueueConsumer(config WorkQueueConfig) (*WorkQueueConsumer, error) {

	// Create JetStream context
	js, err := jetstream.New(config.Conn)
	if err != nil {
		return nil, fmt.Errorf("failed to create JetStream: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	wqc := &WorkQueueConsumer{
		config: &config,
		nc:     config.Conn,
		js:     js,
		ctx:    ctx,
		cancel: cancel,
	}

	// Create or get consumer
	if err := wqc.ensureConsumer(config); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to ensure consumer: %w", err)
	}

	return wqc, nil
}

func (wqc *WorkQueueConsumer) ensureConsumer(config WorkQueueConfig) error {
	// Consumer configuration
	consumerConfig := jetstream.ConsumerConfig{
		Name:           config.ConsumerName,
		Durable:        config.ConsumerName,          // Make consumer durable to survive subscription disconnects
		FilterSubjects: config.Subjects,
		AckPolicy:      jetstream.AckExplicitPolicy, // Require explicit ack
		AckWait:        config.AckWait,              // Set ack wait time
		MaxDeliver:     config.MaxRetries + 1,       // Maximum retry count
		DeliverPolicy:  jetstream.DeliverLastPolicy,
		MaxAckPending:  config.MaxAckPending,
	}

	consumer, err := wqc.js.CreateOrUpdateConsumer(wqc.ctx, config.Stream.Config.Name, consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	wqc.consumer = consumer

	return nil
}

// Start consuming messages
func (wqc *WorkQueueConsumer) Start(handler MessageHandler) error {
	consumeCtx, err := wqc.startConsuming(handler)
	if err != nil {
		return err
	}

	// Wait for cancellation signal
	<-wqc.ctx.Done()

	// Stop consuming
	consumeCtx.Stop()

	return nil
}

// StartAsync begins consuming messages without blocking the caller.
// The returned ConsumeContext can be used to manage the subscription lifecycle.
func (wqc *WorkQueueConsumer) StartAsync(handler MessageHandler) (jetstream.ConsumeContext, error) {
	consumeCtx, err := wqc.startConsuming(handler)
	if err != nil {
		return nil, err
	}

	go func() {
		<-wqc.ctx.Done()
		consumeCtx.Stop()
	}()

	return consumeCtx, nil
}

func (wqc *WorkQueueConsumer) startConsuming(handler MessageHandler) (jetstream.ConsumeContext, error) {
	// Create semaphore to limit concurrency
	semaphore := make(chan struct{}, wqc.config.MaxConcurrent)

	// Limit prefetch so a single instance does not buffer too many messages
	prefetch := wqc.config.MaxConcurrent
	if prefetch <= 0 {
		prefetch = DefaultMaxConcurrent
	}
	if wqc.config.MaxAckPending > 0 && prefetch > wqc.config.MaxAckPending {
		prefetch = wqc.config.MaxAckPending
	}

	// Start consuming
	consumeCtx, err := wqc.consumer.Consume(func(msg jetstream.Msg) {
		// Acquire semaphore (limit concurrency)
		select {
		case semaphore <- struct{}{}:
		case <-wqc.ctx.Done():
			// Shutdown before we could process — nack for immediate redelivery
			_ = msg.Nak()
			return
		}

		wqc.wg.Add(1)
		go func() {
			defer wqc.wg.Done()
			defer func() { <-semaphore }() // Release semaphore

			wqc.processMessage(msg, handler)
		}()
	}, jetstream.PullMaxMessages(prefetch))

	if err != nil {
		return nil, fmt.Errorf("failed to start consuming: %w", err)
	}

	return consumeCtx, nil
}

// handlerContext derives the per-invocation context handed to a message or
// batch handler. It is rooted at wqc.ctx, so Shutdown() always cancels it.
// When config.HandlerTimeout > 0 the context additionally carries that
// deadline; otherwise no deadline is applied (default / legacy behaviour).
func (wqc *WorkQueueConsumer) handlerContext() (context.Context, context.CancelFunc) {
	if wqc.config.HandlerTimeout > 0 {
		return context.WithTimeout(wqc.ctx, wqc.config.HandlerTimeout)
	}
	return context.WithCancel(wqc.ctx)
}

func (wqc *WorkQueueConsumer) processMessage(msg jetstream.Msg, handler MessageHandler) {
	// Create processing context, can be used to pass cancellation signals
	processCtx, cancel := wqc.handlerContext()
	defer cancel()

	// According to MaxAwait to determine interval for sending in progress signal
	interval := wqc.config.AckWait / 3
	if interval < time.Second {
		interval = time.Second // Ensure minimum interval of 1 second
	}

	// Periodically update in progress status to prevent message redelivery
	progressTicker := time.NewTicker(interval) // Update every minute
	defer progressTicker.Stop()

	// Use channel to receive processing result
	done := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("handler panic: %v", r)
			}
		}()
		done <- handler(processCtx, msg)
	}()

	for {
		select {
		case err := <-done:
			// If context was cancelled during processing, always nack
			// to ensure redelivery on restart, regardless of handler result.
			if wqc.ctx.Err() != nil {
				_ = msg.Nak()
				return
			}

			if err != nil {
				wqc.handleError(fmt.Errorf("message processing failed: %w", err))
				// Processing failed, nack with backoff to requeue message
				delay := wqc.nakDelay(msg)
				if nackErr := msg.NakWithDelay(delay); nackErr != nil {
					wqc.handleError(fmt.Errorf("failed to nack message after %s: %w", delay, nackErr))
				}
			} else {
				// Processing successful, ack message
				if ackErr := msg.Ack(); ackErr != nil {
					wqc.handleError(fmt.Errorf("failed to ack message: %w", ackErr))
				}
			}
			return

		case <-progressTicker.C:
			// Periodically send in progress signal to extend ack wait time
			if progressErr := msg.InProgress(); progressErr != nil {
				wqc.handleError(fmt.Errorf("failed to send in progress: %w", progressErr))
			}

		case <-wqc.ctx.Done():
			// Shutdown while processing — nack for immediate redelivery on restart
			_ = msg.Nak()
			return
		}
	}
}

// StartBatch consumes messages in batches of up to BatchSize, calling handler
// once per batch. Acks the whole batch on nil, nacks the whole batch with
// backoff on error. Blocks until Shutdown() is called.
func (wqc *WorkQueueConsumer) StartBatch(handler BatchMessageHandler) error {
	wqc.warnIfBatchExceedsAckPending()
	wqc.runBatchLoop(handler)
	return nil
}

// StartBatchAsync starts the batch consumer in a background goroutine and
// returns immediately. Use Shutdown() / Done() to control its lifecycle.
func (wqc *WorkQueueConsumer) StartBatchAsync(handler BatchMessageHandler) error {
	wqc.warnIfBatchExceedsAckPending()
	wqc.wg.Add(1)
	go func() {
		defer wqc.wg.Done()
		wqc.runBatchLoop(handler)
	}()
	return nil
}

// StartBatchWithRestart runs the batch consumer with automatic restart on
// failure. Mirrors StartWithRestart for the single-message API.
func (wqc *WorkQueueConsumer) StartBatchWithRestart(handler BatchMessageHandler) error {
	attempt := 0
	for {
		attempt++
		startErr := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("batch consumer panic: %v", r)
				}
			}()
			return wqc.StartBatch(handler)
		}()

		select {
		case <-wqc.ctx.Done():
			return nil
		default:
		}

		if startErr != nil {
			wqc.handleError(fmt.Errorf("batch consumer stopped with error (attempt %d): %w", attempt, startErr))
		} else {
			wqc.handleError(fmt.Errorf("batch consumer stopped unexpectedly (attempt %d)", attempt))
		}

		maxRestarts := wqc.config.MaxRestarts
		if maxRestarts == 0 {
			return fmt.Errorf("batch consumer stopped, restarts disabled")
		}
		if maxRestarts > 0 && attempt >= maxRestarts {
			return fmt.Errorf("batch consumer exceeded max restarts (%d)", maxRestarts)
		}

		delay := wqc.restartDelay(attempt)
		wqc.handleError(fmt.Errorf("restarting batch consumer in %s (attempt %d)", delay, attempt))
		select {
		case <-time.After(delay):
		case <-wqc.ctx.Done():
			return nil
		}
	}
}

func (wqc *WorkQueueConsumer) warnIfBatchExceedsAckPending() {
	bs := wqc.config.BatchSize
	if bs <= 0 {
		bs = DefaultBatchSize
	}
	mc := wqc.config.MaxConcurrent
	if mc <= 0 {
		mc = DefaultMaxConcurrent
	}
	if wqc.config.MaxAckPending > 0 && bs*mc > wqc.config.MaxAckPending {
		wqc.handleError(fmt.Errorf(
			"batch in-flight cap (BatchSize=%d * MaxConcurrent=%d = %d) exceeds MaxAckPending=%d; consumer will throttle on ack-pending limit",
			bs, mc, bs*mc, wqc.config.MaxAckPending,
		))
	}
}

func (wqc *WorkQueueConsumer) runBatchLoop(handler BatchMessageHandler) {
	batchSize := wqc.config.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}
	maxWait := wqc.config.BatchMaxWait
	if maxWait <= 0 {
		maxWait = DefaultBatchMaxWait
	}
	maxConcurrent := wqc.config.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrent
	}

	semaphore := make(chan struct{}, maxConcurrent)

	// transientAttempt counts consecutive transient-error retries; reset on
	// any successful fetch (including empty batches — empty means the
	// fetch round completed normally and got no messages).
	transientAttempt := 0

	for {
		select {
		case <-wqc.ctx.Done():
			return
		case semaphore <- struct{}{}:
		}

		batch, err := wqc.fetchBatch(batchSize, maxWait)
		if err != nil {
			<-semaphore
			if wqc.ctx.Err() != nil {
				return
			}
			// Transient cluster errors (cold-start "no responders", leader
			// flap) auto-recover on the next fetch round. Swallow them
			// silently with exponential backoff so we don't flood logs
			// with infrastructure errors that aren't actionable. Only
			// genuine non-transient failures get OnError + the standard
			// maxWait delay.
			if wqc.config.IsTransientError != nil && wqc.config.IsTransientError(err) {
				transientAttempt++
				delay := wqc.transientBackoff(transientAttempt)
				select {
				case <-time.After(delay):
				case <-wqc.ctx.Done():
					return
				}
				continue
			}
			transientAttempt = 0
			wqc.handleError(fmt.Errorf("batch fetch failed: %w", err))
			select {
			case <-time.After(maxWait):
			case <-wqc.ctx.Done():
				return
			}
			continue
		}

		transientAttempt = 0

		if len(batch) == 0 {
			<-semaphore
			continue
		}

		wqc.wg.Add(1)
		go func(msgs []jetstream.Msg) {
			defer wqc.wg.Done()
			defer func() { <-semaphore }()
			wqc.processBatch(msgs, handler)
		}(batch)
	}
}

// transientBackoff returns the delay for the Nth consecutive transient-error
// retry: base * 2^(attempt-1), capped at max, with ±20% jitter.
func (wqc *WorkQueueConsumer) transientBackoff(attempt int) time.Duration {
	base := wqc.config.TransientBackoffBase
	if base <= 0 {
		base = DefaultTransientBackoffBase
	}
	max := wqc.config.TransientBackoffMax
	if max <= 0 {
		max = DefaultTransientBackoffMax
	}

	delay := base * time.Duration(1<<(attempt-1))
	if delay <= 0 || delay > max {
		delay = max
	}

	jitterRange := delay / 5
	if jitterRange < time.Millisecond {
		jitterRange = time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(jitterRange)))

	return delay + jitter
}

func (wqc *WorkQueueConsumer) fetchBatch(batchSize int, maxWait time.Duration) ([]jetstream.Msg, error) {
	mb, err := wqc.consumer.Fetch(batchSize, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		return nil, err
	}

	var batch []jetstream.Msg
	for msg := range mb.Messages() {
		batch = append(batch, msg)
	}
	if ferr := mb.Error(); ferr != nil {
		// Drained messages are still valid; the next fetch round will pick
		// up where this one left off. Suppress transient cluster errors so
		// they don't flood OnError — only genuine non-transient failures
		// get reported.
		if wqc.config.IsTransientError == nil || !wqc.config.IsTransientError(ferr) {
			wqc.handleError(fmt.Errorf("batch fetch reported error: %w", ferr))
		}
	}
	return batch, nil
}

func (wqc *WorkQueueConsumer) processBatch(msgs []jetstream.Msg, handler BatchMessageHandler) {
	if len(msgs) == 0 {
		return
	}

	processCtx, cancel := wqc.handlerContext()
	defer cancel()

	interval := wqc.config.AckWait / 3
	if interval < time.Second {
		interval = time.Second
	}

	progressTicker := time.NewTicker(interval)
	defer progressTicker.Stop()

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("batch handler panic: %v", r)
			}
		}()
		done <- handler(processCtx, msgs)
	}()

	for {
		select {
		case err := <-done:
			if wqc.ctx.Err() != nil {
				for _, m := range msgs {
					_ = m.Nak()
				}
				return
			}

			if err != nil {
				wqc.handleError(fmt.Errorf("batch processing failed: %w", err))
				delay := wqc.nakDelay(msgs[0])
				for _, m := range msgs {
					if nackErr := m.NakWithDelay(delay); nackErr != nil {
						wqc.handleError(fmt.Errorf("failed to nack batch message after %s: %w", delay, nackErr))
					}
				}
			} else {
				for _, m := range msgs {
					if ackErr := m.Ack(); ackErr != nil {
						wqc.handleError(fmt.Errorf("failed to ack batch message: %w", ackErr))
					}
				}
			}
			return

		case <-progressTicker.C:
			for _, m := range msgs {
				if perr := m.InProgress(); perr != nil {
					wqc.handleError(fmt.Errorf("failed to send in progress for batch message: %w", perr))
				}
			}

		case <-wqc.ctx.Done():
			for _, m := range msgs {
				_ = m.Nak()
			}
			return
		}
	}
}

func (wqc *WorkQueueConsumer) nakDelay(msg jetstream.Msg) time.Duration {
	meta, err := msg.Metadata()
	attempt := 1
	if err == nil && meta != nil && meta.NumDelivered > 0 {
		attempt = int(meta.NumDelivered)
	}

	ackWait := wqc.config.AckWait
	if ackWait <= 0 {
		ackWait = DefaultAckWait
	}

	baseDelay := ackWait / 4
	if baseDelay < time.Second {
		baseDelay = time.Second
	}

	maxDelay := ackWait - time.Second
	if maxDelay <= 0 {
		maxDelay = baseDelay
	}

	delay := baseDelay * time.Duration(1<<(attempt-1))
	if delay > maxDelay {
		delay = maxDelay
	}

	jitterRange := delay / 5
	if jitterRange < time.Millisecond {
		jitterRange = time.Millisecond
	}

	jitter := time.Duration(rand.Int63n(int64(jitterRange)))

	finalDelay := delay + jitter
	if finalDelay <= 0 {
		finalDelay = baseDelay
	}

	return finalDelay
}

func (wqc *WorkQueueConsumer) handleError(err error) {
	if err == nil {
		return
	}
	if wqc.config.OnError != nil {
		wqc.config.OnError(err)
	}
}

// Graceful shutdown
func (wqc *WorkQueueConsumer) Shutdown() {

	// Cancel context
	wqc.cancel()

	// Wait for all goroutines to complete
	wqc.wg.Wait()
}

// Done returns a channel that is closed when the consumer context is cancelled.
func (wqc *WorkQueueConsumer) Done() <-chan struct{} {
	return wqc.ctx.Done()
}

func (wqc *WorkQueueConsumer) restartDelay(attempt int) time.Duration {
	baseDelay := wqc.config.RestartBaseDelay
	if baseDelay <= 0 {
		baseDelay = DefaultRestartBaseDelay
	}
	maxDelay := wqc.config.RestartMaxDelay
	if maxDelay <= 0 {
		maxDelay = DefaultRestartMaxDelay
	}

	delay := baseDelay * time.Duration(1<<(attempt-1))
	if delay > maxDelay {
		delay = maxDelay
	}

	jitterRange := delay / 5
	if jitterRange < time.Millisecond {
		jitterRange = time.Millisecond
	}
	jitter := time.Duration(rand.Int63n(int64(jitterRange)))

	finalDelay := delay + jitter
	if finalDelay <= 0 {
		finalDelay = baseDelay
	}
	return finalDelay
}

// StartWithRestart runs the consumer with automatic restart on failure.
// It blocks until the consumer is shut down or max restarts is exceeded.
func (wqc *WorkQueueConsumer) StartWithRestart(handler MessageHandler) error {
	attempt := 0
	for {
		attempt++
		startErr := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("consumer panic: %v", r)
				}
			}()
			return wqc.Start(handler)
		}()

		// Normal shutdown → don't restart
		select {
		case <-wqc.ctx.Done():
			return nil
		default:
		}

		// Report via ErrorHandler
		if startErr != nil {
			wqc.handleError(fmt.Errorf("consumer stopped with error (attempt %d): %w", attempt, startErr))
		} else {
			wqc.handleError(fmt.Errorf("consumer stopped unexpectedly (attempt %d)", attempt))
		}

		// Check restart limit
		maxRestarts := wqc.config.MaxRestarts
		if maxRestarts == 0 {
			return fmt.Errorf("consumer stopped, restarts disabled")
		}
		if maxRestarts > 0 && attempt >= maxRestarts {
			return fmt.Errorf("consumer exceeded max restarts (%d)", maxRestarts)
		}

		// Exponential backoff wait
		delay := wqc.restartDelay(attempt)
		wqc.handleError(fmt.Errorf("restarting consumer in %s (attempt %d)", delay, attempt))
		select {
		case <-time.After(delay):
		case <-wqc.ctx.Done():
			return nil
		}
	}
}
