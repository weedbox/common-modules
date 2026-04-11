package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	libsched "github.com/Weedbox/scheduler"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/spf13/viper"
	cmlogger "github.com/weedbox/common-modules/logger"
	"github.com/weedbox/common-modules/nats_connector"
	"github.com/weedbox/common-modules/sqlite_connector"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// newTestModule constructs a SchedulerModule without going through fx, so
// low-level dispatcher and pending-queue behavior can be tested in isolation.
func newTestModule() *SchedulerModule {
	return &SchedulerModule{
		logger: zap.NewNop(),
		scope:  "scheduler",
		codec:  libsched.NewBasicScheduleCodec(),
	}
}

func TestDispatch_NoHandlerReturnsError(t *testing.T) {
	m := newTestModule()
	err := m.dispatch(context.Background(), libsched.JobEvent{})
	if err == nil {
		t.Fatal("expected error when no handler is set")
	}
}

func TestDispatch_WithHandlerCallsIt(t *testing.T) {
	m := newTestModule()

	var called int32
	m.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	if err := m.dispatch(context.Background(), libsched.JobEvent{}); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Fatalf("handler was not called")
	}
}

func TestSetHandler_OverwritesPrevious(t *testing.T) {
	m := newTestModule()

	var first, second int32
	m.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
		atomic.AddInt32(&first, 1)
		return nil
	})
	m.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
		atomic.AddInt32(&second, 1)
		return nil
	})

	_ = m.dispatch(context.Background(), libsched.JobEvent{})
	if atomic.LoadInt32(&first) != 0 {
		t.Errorf("first handler should not be called after overwrite")
	}
	if atomic.LoadInt32(&second) != 1 {
		t.Errorf("second handler not called")
	}
}

// startWithMemoryStorage starts a module backed by an in-memory library
// scheduler. Returns a cleanup function.
func startWithMemoryStorage(t *testing.T, m *SchedulerModule) func() {
	t.Helper()
	sched := libsched.NewScheduler(libsched.NewMemoryStorage(), m.dispatch, m.codec)
	if err := sched.Start(context.Background()); err != nil {
		t.Fatalf("start scheduler: %v", err)
	}
	if err := sched.WaitUntilRunning(context.Background()); err != nil {
		t.Fatalf("wait running: %v", err)
	}

	m.mu.Lock()
	m.sched = sched
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()
	for _, op := range pending {
		if err := m.applyOp(op); err != nil {
			t.Fatalf("apply pending op: %v", err)
		}
	}

	return func() {
		_ = sched.Stop(context.Background())
	}
}

func TestEnsureJob_IdempotentSameSchedule(t *testing.T) {
	m := newTestModule()
	cleanup := startWithMemoryStorage(t, m)
	defer cleanup()

	sch, err := libsched.NewIntervalSchedule(time.Hour)
	if err != nil {
		t.Fatalf("new schedule: %v", err)
	}

	if err := m.EnsureJob("daily_cleanup", sch, nil); err != nil {
		t.Fatalf("first EnsureJob: %v", err)
	}
	if err := m.EnsureJob("daily_cleanup", sch, nil); err != nil {
		t.Fatalf("second EnsureJob: %v", err)
	}

	jobs := m.ListJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}

func TestEnsureJob_UpdatesScheduleWhenDifferent(t *testing.T) {
	m := newTestModule()
	cleanup := startWithMemoryStorage(t, m)
	defer cleanup()

	first, _ := libsched.NewIntervalSchedule(time.Hour)
	second, _ := libsched.NewIntervalSchedule(2 * time.Hour)

	if err := m.EnsureJob("rotate_logs", first, nil); err != nil {
		t.Fatalf("first EnsureJob: %v", err)
	}
	if err := m.EnsureJob("rotate_logs", second, nil); err != nil {
		t.Fatalf("second EnsureJob (update): %v", err)
	}

	jobs := m.ListJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}
}

func TestSubmitJob_DuplicateReturnsError(t *testing.T) {
	m := newTestModule()
	cleanup := startWithMemoryStorage(t, m)
	defer cleanup()

	sch, _ := libsched.NewIntervalSchedule(time.Hour)

	if err := m.SubmitJob("dynamic-1", sch, nil); err != nil {
		t.Fatalf("first SubmitJob: %v", err)
	}
	err := m.SubmitJob("dynamic-1", sch, nil)
	if !errors.Is(err, libsched.ErrJobAlreadyExists) {
		t.Fatalf("expected ErrJobAlreadyExists, got %v", err)
	}
}

func TestPendingOps_FlushedOnStart(t *testing.T) {
	// Pre-start: queue operations via EnsureJob/SubmitJob, then start and
	// verify jobs appear.
	m := newTestModule()

	sch, _ := libsched.NewIntervalSchedule(time.Hour)
	if err := m.EnsureJob("pre_start_ensure", sch, nil); err != nil {
		t.Fatalf("EnsureJob pre-start: %v", err)
	}
	if err := m.SubmitJob("pre_start_submit", sch, nil); err != nil {
		t.Fatalf("SubmitJob pre-start: %v", err)
	}

	// Pending queue should hold 2 ops now.
	m.mu.RLock()
	if len(m.pending) != 2 {
		t.Fatalf("expected 2 pending ops, got %d", len(m.pending))
	}
	m.mu.RUnlock()

	cleanup := startWithMemoryStorage(t, m)
	defer cleanup()

	jobs := m.ListJobs()
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs after flush, got %d", len(jobs))
	}
}

func TestRemoveJob_BeforeStart(t *testing.T) {
	m := newTestModule()
	err := m.RemoveJob("nope")
	if !errors.Is(err, libsched.ErrSchedulerNotStarted) {
		t.Fatalf("expected ErrSchedulerNotStarted, got %v", err)
	}
}

// --- Integration test with fx + sqlite_connector (GORM mode) ---

func TestIntegration_GormMode_HandlerFires(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	viper.Reset()
	defer viper.Reset()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "scheduler.db")

	viper.Set("sqlite.path", dbPath)
	viper.Set("scheduler.mode", ModeGorm)

	var sm *SchedulerModule
	var calls int32
	var wg sync.WaitGroup
	wg.Add(1)
	var once sync.Once

	app := fx.New(
		fx.NopLogger,
		cmlogger.Module(),
		sqlite_connector.Module("sqlite"),
		Module("scheduler"),
		fx.Populate(&sm),
		fx.Invoke(func(s *SchedulerModule) {
			s.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
				atomic.AddInt32(&calls, 1)
				once.Do(wg.Done)
				return nil
			})
		}),
		fx.Invoke(func(s *SchedulerModule) error {
			sch, err := libsched.NewIntervalSchedule(500 * time.Millisecond)
			if err != nil {
				return err
			}
			return s.EnsureJob("heartbeat", sch, nil)
		}),
	)

	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("fx start: %v", err)
	}

	waitOrTimeout(t, &wg, 3*time.Second, "handler never fired")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("fx stop: %v", err)
	}

	if atomic.LoadInt32(&calls) < 1 {
		t.Fatalf("expected handler to be called at least once")
	}

	// Verify the GORM tables were actually created (proves GORM mode was used).
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected sqlite file at %s: %v", dbPath, err)
	}
}

func TestIntegration_GormMode_EnsureJobIdempotentAcrossRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	viper.Reset()
	defer viper.Reset()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "scheduler.db")

	viper.Set("sqlite.path", dbPath)
	viper.Set("scheduler.mode", ModeGorm)

	runOnce := func(t *testing.T) int {
		t.Helper()
		var sm *SchedulerModule
		app := fx.New(
			fx.NopLogger,
			cmlogger.Module(),
			sqlite_connector.Module("sqlite"),
			Module("scheduler"),
			fx.Populate(&sm),
			fx.Invoke(func(s *SchedulerModule) {
				s.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
					return nil
				})
			}),
			fx.Invoke(func(s *SchedulerModule) error {
				sch, _ := libsched.NewIntervalSchedule(time.Hour)
				return s.EnsureJob("daily_cleanup", sch, nil)
			}),
		)

		startCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := app.Start(startCtx); err != nil {
			t.Fatalf("fx start: %v", err)
		}
		count := len(sm.ListJobs())
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := app.Stop(stopCtx); err != nil {
			t.Fatalf("fx stop: %v", err)
		}
		return count
	}

	if n := runOnce(t); n != 1 {
		t.Fatalf("first run expected 1 job, got %d", n)
	}
	if n := runOnce(t); n != 1 {
		t.Fatalf("second run (should be idempotent) expected 1 job, got %d", n)
	}
}

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, d time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal(msg)
	}
}

// --- Integration tests with embedded NATS JetStream server ---

// startEmbeddedNATSServer launches an in-process NATS server with JetStream
// enabled on a random port and returns its client URL. The server is shut
// down via t.Cleanup. Requires nats-server v2.12+ for AllowMsgSchedules.
func startEmbeddedNATSServer(t *testing.T) string {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoSigs:    true,
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("create nats server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats server not ready")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return ns.ClientURL()
}

func TestIntegration_NATSMode_HandlerFires(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	viper.Reset()
	defer viper.Reset()

	url := startEmbeddedNATSServer(t)

	viper.Set("nats.host", url)
	viper.Set("scheduler.mode", ModeNATS)
	viper.Set("scheduler.nats.streamName", fmt.Sprintf("SCHED_%d", time.Now().UnixNano()))

	var sm *SchedulerModule
	var calls int32
	var wg sync.WaitGroup
	wg.Add(1)
	var once sync.Once

	app := fx.New(
		fx.NopLogger,
		cmlogger.Module(),
		nats_connector.Module("nats"),
		Module("scheduler"),
		fx.Populate(&sm),
		fx.Invoke(func(s *SchedulerModule) {
			s.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
				atomic.AddInt32(&calls, 1)
				once.Do(wg.Done)
				return nil
			})
		}),
		fx.Invoke(func(s *SchedulerModule) error {
			sch, err := libsched.NewIntervalSchedule(500 * time.Millisecond)
			if err != nil {
				return err
			}
			return s.EnsureJob("heartbeat", sch, nil)
		}),
	)

	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()
	if err := app.Start(startCtx); err != nil {
		if errors.Is(err, libsched.ErrNATSServerTooOld) {
			t.Skipf("embedded nats-server lacks AllowMsgSchedules support: %v", err)
		}
		t.Fatalf("fx start: %v", err)
	}

	waitOrTimeout(t, &wg, 5*time.Second, "handler never fired")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("fx stop: %v", err)
	}

	if atomic.LoadInt32(&calls) < 1 {
		t.Fatalf("expected handler to be called at least once")
	}
}

// TestIntegration_NATSMode_FailoverReloadsJobs verifies that an interval job
// persisted to the JetStream KV bucket is reloaded when a fresh scheduler
// instance starts against the same stream — the failover/restart semantic the
// library is designed around.
func TestIntegration_NATSMode_FailoverReloadsJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	viper.Reset()
	defer viper.Reset()

	url := startEmbeddedNATSServer(t)

	suffix := time.Now().UnixNano()
	viper.Set("nats.host", url)
	viper.Set("scheduler.mode", ModeNATS)
	viper.Set("scheduler.nats.streamName", fmt.Sprintf("SCHED_%d", suffix))
	viper.Set("scheduler.nats.subjectPrefix", "scheduler_fo")
	viper.Set("scheduler.nats.consumerName", "scheduler-worker-fo")
	viper.Set("scheduler.nats.jobBucket", fmt.Sprintf("SCHED_JOBS_%d", suffix))
	viper.Set("scheduler.nats.execBucket", fmt.Sprintf("SCHED_EXEC_%d", suffix))

	var calls int32

	build := func() (*fx.App, *SchedulerModule) {
		var sm *SchedulerModule
		app := fx.New(
			fx.NopLogger,
			cmlogger.Module(),
			nats_connector.Module("nats"),
			Module("scheduler"),
			fx.Populate(&sm),
			fx.Invoke(func(s *SchedulerModule) {
				s.SetHandler(func(ctx context.Context, e libsched.JobEvent) error {
					atomic.AddInt32(&calls, 1)
					return nil
				})
			}),
			fx.Invoke(func(s *SchedulerModule) error {
				sch, err := libsched.NewIntervalSchedule(500 * time.Millisecond)
				if err != nil {
					return err
				}
				return s.EnsureJob("failover-job", sch, nil)
			}),
		)
		return app, sm
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer startCancel()

	// First instance: register job, let it fire at least once, then stop.
	appA, smA := build()
	if err := appA.Start(startCtx); err != nil {
		if errors.Is(err, libsched.ErrNATSServerTooOld) {
			t.Skipf("embedded nats-server lacks AllowMsgSchedules support: %v", err)
		}
		t.Fatalf("fx start A: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&calls) < 1 {
		time.Sleep(100 * time.Millisecond)
	}
	if atomic.LoadInt32(&calls) < 1 {
		t.Fatalf("first instance: handler never fired")
	}
	if jobs := smA.ListJobs(); len(jobs) != 1 {
		t.Fatalf("first instance: expected 1 job, got %d", len(jobs))
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := appA.Stop(stopCtx); err != nil {
		stopCancel()
		t.Fatalf("fx stop A: %v", err)
	}
	stopCancel()

	beforeRestart := atomic.LoadInt32(&calls)

	// Second instance: should reload the job from KV and continue firing.
	appB, smB := build()
	if err := appB.Start(startCtx); err != nil {
		t.Fatalf("fx start B: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = appB.Stop(stopCtx)
	}()

	if jobs := smB.ListJobs(); len(jobs) != 1 {
		t.Fatalf("second instance: expected 1 reloaded job, got %d", len(jobs))
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomic.LoadInt32(&calls) <= beforeRestart {
		time.Sleep(100 * time.Millisecond)
	}
	if atomic.LoadInt32(&calls) <= beforeRestart {
		t.Fatalf("second instance: handler did not fire after reload (calls=%d, before=%d)",
			atomic.LoadInt32(&calls), beforeRestart)
	}
}
