package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"

	libsched "github.com/Weedbox/scheduler"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
	"github.com/weedbox/common-modules/database"
	"github.com/weedbox/common-modules/nats_connector"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	ModeGorm = "gorm"
	ModeNATS = "nats"

	DefaultMode          = ModeGorm
	DefaultStreamName    = "SCHEDULER"
	DefaultSubjectPrefix = "scheduler"
	DefaultConsumerName  = "scheduler-worker"
	DefaultJobBucket     = "SCHEDULER_JOBS"
	DefaultExecBucket    = "SCHEDULER_EXECUTIONS"
)

var logger *zap.Logger

// ErrModeNotConfigured indicates required dependency for the selected mode is missing.
var ErrModeNotConfigured = errors.New("scheduler: required dependency not provided for selected mode")

type pendingOpKind int

const (
	opEnsure pendingOpKind = iota
	opSubmit
)

type pendingOp struct {
	kind     pendingOpKind
	id       string
	schedule libsched.Schedule
	metadata map[string]string
}

type SchedulerModule struct {
	logger *zap.Logger
	scope  string

	mu      sync.RWMutex
	handler libsched.JobHandler
	sched   libsched.Scheduler
	codec   libsched.ScheduleCodec
	pending []pendingOp

	nats *nats_connector.NATSConnector
	db   database.DatabaseConnector
}

type Params struct {
	fx.In

	Lifecycle fx.Lifecycle
	Logger    *zap.Logger
	NATS      *nats_connector.NATSConnector `optional:"true"`
	DB        database.DatabaseConnector    `optional:"true"`
}

func Module(scope string) fx.Option {
	var sm *SchedulerModule

	return fx.Module(
		scope,
		fx.Provide(func(p Params) *SchedulerModule {
			logger = p.Logger.Named(scope)

			sm = &SchedulerModule{
				logger: logger,
				scope:  scope,
				codec:  libsched.NewBasicScheduleCodec(),
				nats:   p.NATS,
				db:     p.DB,
			}

			sm.initDefaultConfigs()

			return sm
		}),
		fx.Populate(&sm),
		fx.Invoke(func(p Params) {
			p.Lifecycle.Append(fx.Hook{
				OnStart: sm.onStart,
				OnStop:  sm.onStop,
			})
		}),
	)
}

func (m *SchedulerModule) getConfigPath(key string) string {
	return fmt.Sprintf("%s.%s", m.scope, key)
}

func (m *SchedulerModule) initDefaultConfigs() {
	viper.SetDefault(m.getConfigPath("mode"), DefaultMode)
	viper.SetDefault(m.getConfigPath("nats.streamName"), DefaultStreamName)
	viper.SetDefault(m.getConfigPath("nats.subjectPrefix"), DefaultSubjectPrefix)
	viper.SetDefault(m.getConfigPath("nats.consumerName"), DefaultConsumerName)
	viper.SetDefault(m.getConfigPath("nats.jobBucket"), DefaultJobBucket)
	viper.SetDefault(m.getConfigPath("nats.execBucket"), DefaultExecBucket)
}

func (m *SchedulerModule) onStart(ctx context.Context) error {
	mode := viper.GetString(m.getConfigPath("mode"))

	m.logger.Info("Starting SchedulerModule", zap.String("mode", mode))

	var sched libsched.Scheduler

	switch mode {
	case ModeNATS:
		if m.nats == nil {
			return fmt.Errorf("%w: nats mode requires nats_connector", ErrModeNotConfigured)
		}
		nc := m.nats.GetConnection()
		if nc == nil {
			return fmt.Errorf("%w: nats connection not initialized", ErrModeNotConfigured)
		}
		js, err := jetstream.New(nc)
		if err != nil {
			return fmt.Errorf("scheduler: failed to create jetstream context: %w", err)
		}
		opts := []libsched.NATSSchedulerOption{
			libsched.WithNATSStreamName(viper.GetString(m.getConfigPath("nats.streamName"))),
			libsched.WithNATSSubjectPrefix(viper.GetString(m.getConfigPath("nats.subjectPrefix"))),
			libsched.WithNATSConsumerName(viper.GetString(m.getConfigPath("nats.consumerName"))),
			libsched.WithNATSSchedulerJobBucket(viper.GetString(m.getConfigPath("nats.jobBucket"))),
			libsched.WithNATSSchedulerExecBucket(viper.GetString(m.getConfigPath("nats.execBucket"))),
			libsched.WithNATSSchedulerCodec(m.codec),
		}
		sched = libsched.NewNATSScheduler(js, m.dispatch, opts...)

	case ModeGorm:
		if m.db == nil {
			return fmt.Errorf("%w: gorm mode requires database connector", ErrModeNotConfigured)
		}
		gdb := m.db.GetDB()
		if gdb == nil {
			return fmt.Errorf("%w: database not initialized", ErrModeNotConfigured)
		}
		sched = libsched.NewScheduler(libsched.NewGormStorage(gdb), m.dispatch, m.codec)

	default:
		return fmt.Errorf("scheduler: unknown mode %q", mode)
	}

	if err := sched.Start(ctx); err != nil {
		return fmt.Errorf("scheduler: failed to start: %w", err)
	}
	if err := sched.WaitUntilRunning(ctx); err != nil {
		return fmt.Errorf("scheduler: failed to reach running state: %w", err)
	}

	m.mu.Lock()
	m.sched = sched
	pending := m.pending
	m.pending = nil
	m.mu.Unlock()

	for _, op := range pending {
		if err := m.applyOp(op); err != nil {
			m.logger.Error("Failed to apply pending scheduler op",
				zap.String("id", op.id), zap.Error(err))
			return err
		}
	}

	m.logger.Info("SchedulerModule started")
	return nil
}

func (m *SchedulerModule) onStop(ctx context.Context) error {
	m.mu.RLock()
	sched := m.sched
	m.mu.RUnlock()

	if sched == nil {
		return nil
	}

	if err := sched.Stop(ctx); err != nil {
		return err
	}
	m.logger.Info("SchedulerModule stopped")
	return nil
}

// dispatch is the single JobHandler handed to the underlying library. It
// forwards events to the user-provided handler set via SetHandler. This
// indirection lets SetHandler be called before or after Start.
func (m *SchedulerModule) dispatch(ctx context.Context, e libsched.JobEvent) error {
	m.mu.RLock()
	h := m.handler
	m.mu.RUnlock()
	if h == nil {
		return fmt.Errorf("scheduler: no handler set (job id=%s)", e.ID())
	}
	return h(ctx, e)
}

// SetHandler sets the global job handler. Typically called inside fx.Invoke
// at startup. Multiple calls overwrite the previous handler.
func (m *SchedulerModule) SetHandler(h libsched.JobHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

// EnsureJob registers a job idempotently. If the job ID already exists, its
// schedule is updated. If it does not exist, the job is added. Safe to call
// from fx.Invoke: if the scheduler has not started yet, the operation is
// queued and applied during OnStart.
func (m *SchedulerModule) EnsureJob(id string, schedule libsched.Schedule, metadata map[string]string) error {
	m.mu.Lock()
	if m.sched == nil {
		m.pending = append(m.pending, pendingOp{
			kind:     opEnsure,
			id:       id,
			schedule: schedule,
			metadata: copyMetadata(metadata),
		})
		m.mu.Unlock()
		return nil
	}
	sched := m.sched
	m.mu.Unlock()

	return ensureJob(sched, id, schedule, metadata)
}

// SubmitJob adds a one-shot or dynamically created job. Returns
// libsched.ErrJobAlreadyExists if the ID collides. Also supports being
// called before OnStart via the pending queue.
func (m *SchedulerModule) SubmitJob(id string, schedule libsched.Schedule, metadata map[string]string) error {
	m.mu.Lock()
	if m.sched == nil {
		m.pending = append(m.pending, pendingOp{
			kind:     opSubmit,
			id:       id,
			schedule: schedule,
			metadata: copyMetadata(metadata),
		})
		m.mu.Unlock()
		return nil
	}
	sched := m.sched
	m.mu.Unlock()

	return sched.AddJob(id, schedule, metadata)
}

// RemoveJob removes a job by ID. Only valid after the scheduler has started.
func (m *SchedulerModule) RemoveJob(id string) error {
	m.mu.RLock()
	sched := m.sched
	m.mu.RUnlock()
	if sched == nil {
		return libsched.ErrSchedulerNotStarted
	}
	return sched.RemoveJob(id)
}

// GetJob looks up a job by ID. Only valid after the scheduler has started.
func (m *SchedulerModule) GetJob(id string) (libsched.Job, error) {
	m.mu.RLock()
	sched := m.sched
	m.mu.RUnlock()
	if sched == nil {
		return nil, libsched.ErrSchedulerNotStarted
	}
	return sched.GetJob(id)
}

// ListJobs returns all registered jobs. Returns nil if the scheduler has
// not started yet.
func (m *SchedulerModule) ListJobs() []libsched.Job {
	m.mu.RLock()
	sched := m.sched
	m.mu.RUnlock()
	if sched == nil {
		return nil
	}
	return sched.ListJobs()
}

// GetScheduler returns the underlying library Scheduler. Use only when the
// wrapper API is insufficient. Returns nil if called before OnStart.
func (m *SchedulerModule) GetScheduler() libsched.Scheduler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sched
}

func (m *SchedulerModule) applyOp(op pendingOp) error {
	switch op.kind {
	case opEnsure:
		return ensureJob(m.sched, op.id, op.schedule, op.metadata)
	case opSubmit:
		return m.sched.AddJob(op.id, op.schedule, op.metadata)
	}
	return fmt.Errorf("scheduler: unknown pending op kind %d", op.kind)
}

func ensureJob(sched libsched.Scheduler, id string, schedule libsched.Schedule, metadata map[string]string) error {
	_, err := sched.GetJob(id)
	if errors.Is(err, libsched.ErrJobNotFound) {
		return sched.AddJob(id, schedule, metadata)
	}
	if err != nil {
		return err
	}
	return sched.UpdateJobSchedule(id, schedule)
}

func copyMetadata(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
