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
	QueueLoad() float64
	QueueLen() int
	QueueCap() int
}

// 调度公平性与背压阈值
const (
	// 队列负载超过此阈值，停止拉取 FAILED 重试任务（只放行 PENDING 新任务）
	highLoadThreshold = 0.7
	// 队列负载超过此阈值，连 PENDING 也停止拉取（极端背压）
	extremeLoadThreshold = 0.95
	// 新任务配额占比：PENDING 永远先占 BatchSize 的 pendingRatio（保留带宽给新任务）
	pendingRatio = 0.7
)

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

// tick 单次扫描：先回收卡死任务，再按队列水位与优先级规则调度。
// 核心公平性规则：
//   - 负载 > 95% ：不拉任何任务（让系统先排空）
//   - 负载 > 70% ：只拉 PENDING 新任务（重试让路）
//   - 负载 <= 70%：PENDING 先占 70% 配额，剩余配额给 FAILED 重试
func (s *Scheduler) tick(ctx context.Context) {
	s.reapStuck(ctx)

	load := s.dispatcher.QueueLoad()
	// 极端背压：完全暂停调度，让 worker 先把队列清空
	if load >= extremeLoadThreshold {
		s.log.Warn("queue under extreme pressure, skip tick",
			zap.Int("queue_len", s.dispatcher.QueueLen()),
			zap.Int("queue_cap", s.dispatcher.QueueCap()),
			zap.Float64("load", load))
		return
	}

	now := time.Now()
	pendingQuota := int(float64(s.cfg.BatchSize) * pendingRatio)
	remainingQuota := s.cfg.BatchSize

	// --- 阶段 1：PENDING 新任务，永远优先 ---
	pending, err := s.taskRepo.FetchPending(ctx, now, pendingQuota)
	if err != nil {
		s.log.Error("fetch pending tasks failed", zap.Error(err))
		// PENDING 失败不阻断 FAILED，继续往下
	} else {
		pendingSubmitted := s.batchSubmit(ctx, pending)
		remainingQuota -= pendingSubmitted
	}

	// --- 阶段 2：FAILED 重试任务，仅在低负载时放行 ---
	if load < highLoadThreshold && remainingQuota > 0 {
		failed, err := s.taskRepo.FetchFailed(ctx, now, remainingQuota)
		if err != nil {
			s.log.Error("fetch failed tasks failed", zap.Error(err))
		} else {
			s.batchSubmit(ctx, failed)
		}
	} else if load >= highLoadThreshold {
		s.log.Info("skip fetching failed tasks under high load",
			zap.Float64("load", load),
			zap.Float64("high_load_threshold", highLoadThreshold))
	}
}

// batchSubmit 批量提交到队列，返回实际入队数。
func (s *Scheduler) batchSubmit(ctx context.Context, tasks []model.Task) int {
	submitted := 0
	for _, t := range tasks {
		if s.dispatcher.Submit(ctx, t) {
			submitted++
		} else {
			// 队列满，剩下的全部丢弃（下个 tick 再来），避免反复尝试
			s.log.Debug("queue full, drop remaining tasks in batch",
				zap.String("task_key", t.TaskKey),
				zap.Int("dropped_total", len(tasks)-submitted))
			break
		}
	}
	return submitted
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
