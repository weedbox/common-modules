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
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewWorkQueueConsumerConfig() WorkQueueConfig {
	return WorkQueueConfig{
		Conn:          nil, // Connection will be set later
		Stream:        nil, // Stream will be set later
		ConsumerName:  "default_consumer",
		Subjects:      []string{"work_queue"},
		MaxConcurrent: DefaultMaxConcurrent,
		AckWait:       DefaultAckWait,
		MaxRetries:    DefaultMaxRetries, // Default to unlimited retries
		MaxAckPending:    DefaultMaxAckPending,
		OnError:          nil,
		MaxRestarts:      DefaultMaxRestarts,
		RestartBaseDelay: DefaultRestartBaseDelay,
		RestartMaxDelay:  DefaultRestartMaxDelay,
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

func (wqc *WorkQueueConsumer) processMessage(msg jetstream.Msg, handler MessageHandler) {
	// Create processing context, can be used to pass cancellation signals
	processCtx, cancel := context.WithCancel(wqc.ctx)
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
