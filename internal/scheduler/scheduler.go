package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"ct6/internal/config"
	"ct6/internal/model"
	"ct6/internal/repository"
	"ct6/pkg/logger"
)

// Dispatcher 由 Dispatcher 层实现，Scheduler 仅负责“扫描到期任务并投递”。
type Dispatcher interface {
	Submit(ctx context.Context, task model.Task) bool
}

type Scheduler struct {
	taskRepo    repository.TaskRepository
	dispatcher  Dispatcher
	cfg         config.SchedulerConfig
	dispatchCfg config.DispatcherConfig
	instanceID  string
	log         *zap.Logger

	tickCh chan struct{}
	stopCh chan struct{}
}

func NewScheduler(
	taskRepo repository.TaskRepository,
	dispatcher Dispatcher,
	schedCfg config.SchedulerConfig,
	dispatchCfg config.DispatcherConfig,
	instanceID string,
) *Scheduler {
	return &Scheduler{
		taskRepo:    taskRepo,
		dispatcher:  dispatcher,
		cfg:         schedCfg,
		dispatchCfg: dispatchCfg,
		instanceID:  instanceID,
		log:         logger.L().Named("scheduler"),
	}
}

// RegisterTask 幂等注册任务。重复提交相同 task_key 返回既有任务且 created=false。
func (s *Scheduler) RegisterTask(ctx context.Context, in model.RegisterTaskInput) (*model.Task, bool, error) {
	if err := s.validate(in); err != nil {
		return nil, false, err
	}

	headersJSON, err := marshalHeaders(in.Headers)
	if err != nil {
		return nil, false, fmt.Errorf("marshal headers: %w", err)
	}

	method := in.Method
	if method == "" {
		method = "POST"
	}
	maxRetries := in.MaxRetries
	if maxRetries <= 0 {
		maxRetries = s.dispatchCfg.MaxRetries
	}

	task := &model.Task{
		TaskKey:    in.TaskKey,
		WebhookURL: in.WebhookURL,
		Method:     method,
		Headers:    headersJSON,
		Payload:    in.Payload,
		MaxRetries: maxRetries,
		Attempt:    0,
		State:      model.StatePending,
		NextRunAt:  time.Now(),
		Priority:   in.Priority,
	}

	existing, created, err := s.taskRepo.Register(ctx, task)
	if err != nil {
		return nil, false, fmt.Errorf("register task: %w", err)
	}
	return existing, created, nil
}

func (s *Scheduler) validate(in model.RegisterTaskInput) error {
	if in.TaskKey == "" {
		return errors.New("task_key is required")
	}
	if in.WebhookURL == "" {
		return errors.New("webhook_url is required")
	}
	return nil
}

// Start 启动调度循环。阻塞直到 ctx 取消或 Stop 被调用。
func (s *Scheduler) Start(ctx context.Context) {
	s.tickCh = make(chan struct{}, 1)
	s.stopCh = make(chan struct{})

	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	s.log.Info("scheduler started",
		zap.String("instance", s.instanceID),
		zap.Duration("tick", s.cfg.TickInterval),
		zap.Int("batch", s.cfg.BatchSize))

	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopping (context cancelled)")
			return
		case <-s.stopCh:
			s.log.Info("scheduler stopping (stop signal)")
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

// Stop 通知调度循环退出。
func (s *Scheduler) Stop() {
	if s.stopCh != nil {
		select {
		case <-s.stopCh:
		default:
			close(s.stopCh)
		}
	}
}

// tick 单次扫描：先回收卡死的 dispatched 任务，再拉取到期任务并提交给 Dispatcher。
func (s *Scheduler) tick(ctx context.Context) {
	s.reapStuck(ctx)

	now := time.Now()
	tasks, err := s.taskRepo.FetchDispatchable(ctx, now, s.cfg.BatchSize)
	if err != nil {
		s.log.Error("fetch dispatchable tasks failed", zap.Error(err))
		return
	}
	if len(tasks) == 0 {
		return
	}

	submitted, dropped := 0, 0
	for _, t := range tasks {
		// Submit 内部做背压：通道满则返回 false，跳过本次（下个 tick 再取）。
		if s.dispatcher.Submit(ctx, t) {
			submitted++
		} else {
			dropped++
		}
	}
	s.log.Info("tick dispatched",
		zap.Int("fetched", len(tasks)),
		zap.Int("submitted", submitted),
		zap.Int("backpressure_dropped", dropped))
}

// reapStuck 回收因实例崩溃而卡在 dispatched 状态的任务。
// 阈值取 ClaimTTL 的 3 倍，确保远大于单次投递耗时，避免误回收正在处理中的任务。
func (s *Scheduler) reapStuck(ctx context.Context) {
	threshold := time.Now().Add(-s.cfg.ClaimTTL * 3)
	reset, err := s.taskRepo.ResetStuckDispatched(ctx, threshold, time.Now())
	if err != nil {
		s.log.Error("reap stuck dispatched tasks failed", zap.Error(err))
		return
	}
	if reset > 0 {
		s.log.Warn("reaped stuck dispatched tasks",
			zap.Int64("count", reset),
			zap.Duration("threshold", s.cfg.ClaimTTL*3))
	}
}

func marshalHeaders(h map[string]string) (string, error) {
	if len(h) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
