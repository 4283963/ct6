package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"ct6/internal/config"
	"ct6/internal/lock"
	"ct6/internal/model"
	"ct6/internal/repository"
	"ct6/pkg/httpclient"
	"ct6/pkg/logger"
	"ct6/pkg/retry"
)

type Dispatcher struct {
	taskRepo   repository.TaskRepository
	execRepo   repository.ExecutionRepository
	locker     lock.Locker
	httpClient *httpclient.Client
	cfg        config.DispatcherConfig
	instanceID string
	backoff    *retry.Backoff
	workers    int
	log        *zap.Logger

	queue  chan model.Task
	stopCh chan struct{}
}

func NewDispatcher(
	taskRepo repository.TaskRepository,
	execRepo repository.ExecutionRepository,
	locker lock.Locker,
	cfg config.DispatcherConfig,
	schedCfg config.SchedulerConfig,
	instanceID string,
) *Dispatcher {
	d := &Dispatcher{
		taskRepo:   taskRepo,
		execRepo:   execRepo,
		locker:     locker,
		httpClient: httpclient.New(cfg.HTTPTimeout),
		cfg:        cfg,
		instanceID: instanceID,
		backoff: retry.NewBackoff(
			cfg.BaseBackoff,
			cfg.MaxBackoff,
			cfg.BackoffMultiplier,
			cfg.JitterRatio,
		),
		log:     logger.L().Named("dispatcher"),
		queue:   make(chan model.Task, schedCfg.MaxInFlight),
		workers: schedCfg.WorkerCount,
		stopCh:  make(chan struct{}),
	}
	return d
}

// Start 启动 worker 池，阻塞直到 ctx 取消。
func (d *Dispatcher) Start(ctx context.Context) {
	d.log.Info("dispatcher started",
		zap.String("instance", d.instanceID),
		zap.Int("workers", d.workers),
		zap.Int("queue_size", cap(d.queue)))

	for i := 0; i < d.workers; i++ {
		go d.worker(ctx, i)
	}
	<-ctx.Done()
	d.log.Info("dispatcher stopping (context cancelled)")
}

// Submit 非阻塞投递任务到队列。队列满时返回 false（背压）。
func (d *Dispatcher) Submit(ctx context.Context, task model.Task) bool {
	select {
	case d.queue <- task:
		return true
	default:
		return false
	}
}

func (d *Dispatcher) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case task := <-d.queue:
			d.processTask(ctx, task)
		}
	}
}

// Stop 通知所有 worker 退出。
func (d *Dispatcher) Stop() {
	select {
	case <-d.stopCh:
	default:
		close(d.stopCh)
	}
}

// processTask 单个任务的核心处理流程，严格保证“不重复分发/消费”：
//  1. Redis 分布式锁：跨实例互斥（快速失败路径）
//  2. DB 条件 UPDATE（ClaimForDispatch）：权威幂等屏障
//  3. 幂等键透传给消费端：消费端基于 X-Idempotency-Key 去重
//  4. 执行记录以 delivery_token 唯一索引落库：审计层兜底
func (d *Dispatcher) processTask(ctx context.Context, task model.Task) {
	token, err := lock.NewToken(d.instanceID)
	if err != nil {
		d.log.Error("generate lock token failed", zap.String("task_key", task.TaskKey), zap.Error(err))
		return
	}

	lockKey := "dispatch:" + task.TaskKey
	rel, err := d.locker.AcquireWithRetry(ctx, lockKey, token, d.cfg.LockTTL, 100*time.Millisecond, 2*time.Second)
	if err != nil {
		d.log.Debug("lock not acquired, skip (another instance handling)",
			zap.String("task_key", task.TaskKey))
		return
	}
	defer func() {
		if _, err := rel(context.Background()); err != nil {
			d.log.Warn("release lock failed", zap.String("task_key", task.TaskKey), zap.Error(err))
		}
	}()

	// 权威幂等屏障：仅当状态仍为 pending/failed 时才迁移到 dispatched。
	if err := d.taskRepo.ClaimForDispatch(ctx, task.TaskKey, []model.TaskState{model.StatePending, model.StateFailed}); err != nil {
		if errors.Is(err, repository.ErrNotClaimed) {
			d.log.Debug("task not claimed (state changed), skip",
				zap.String("task_key", task.TaskKey))
			return
		}
		d.log.Error("claim task failed", zap.String("task_key", task.TaskKey), zap.Error(err))
		return
	}

	thisAttempt := task.Attempt + 1
	deliveryToken := fmt.Sprintf("%s#%d", task.TaskKey, thisAttempt)
	headers := d.buildHeaders(task, deliveryToken, thisAttempt)

	reqCtx, cancel := context.WithTimeout(ctx, d.cfg.HTTPTimeout)
	defer cancel()
	result := d.httpClient.Do(reqCtx, task.Method, task.WebhookURL, headers, task.Payload)

	d.recordExecution(ctx, task.TaskKey, thisAttempt, deliveryToken, result)

	if result.IsSuccess() {
		if err := d.taskRepo.MarkSucceeded(ctx, task.TaskKey); err != nil {
			d.log.Error("mark succeeded failed", zap.String("task_key", task.TaskKey), zap.Error(err))
			return
		}
		d.log.Info("task delivered",
			zap.String("task_key", task.TaskKey),
			zap.Int("attempt", thisAttempt),
			zap.Int("status", result.StatusCode),
			zap.Duration("dur", result.Duration))
		return
	}

	d.handleFailure(ctx, task, thisAttempt, result)
}

// handleFailure 处理投递失败：决定重试（指数退避）或进入死信。
func (d *Dispatcher) handleFailure(ctx context.Context, task model.Task, thisAttempt int, result httpclient.Result) {
	errMsg := truncate(errorMessage(result), 1024)
	maxRetries := task.MaxRetries
	if maxRetries <= 0 {
		maxRetries = d.cfg.MaxRetries
	}
	retriesUsed := thisAttempt - 1

	if retriesUsed >= maxRetries {
		if err := d.taskRepo.MarkDead(ctx, task.TaskKey, thisAttempt, errMsg); err != nil {
			d.log.Error("mark dead failed", zap.String("task_key", task.TaskKey), zap.Error(err))
			return
		}
		d.log.Warn("task dead (retries exhausted)",
			zap.String("task_key", task.TaskKey),
			zap.Int("attempt", thisAttempt),
			zap.Int("max_retries", maxRetries))
		return
	}

	nextRun := time.Now().Add(d.backoff.Next(thisAttempt))
	if err := d.taskRepo.MarkFailed(ctx, task.TaskKey, thisAttempt, nextRun, errMsg); err != nil {
		d.log.Error("mark failed failed", zap.String("task_key", task.TaskKey), zap.Error(err))
		return
	}
	d.log.Info("task failed, scheduled retry",
		zap.String("task_key", task.TaskKey),
		zap.Int("attempt", thisAttempt),
		zap.Int("status", result.StatusCode),
		zap.Time("next_run_at", nextRun))
}

func (d *Dispatcher) recordExecution(ctx context.Context, taskKey string, attempt int, token string, result httpclient.Result) {
	exec := &model.TaskExecution{
		TaskKey:       taskKey,
		Attempt:       attempt,
		DeliveryToken: token,
		StatusCode:    result.StatusCode,
		ResponseBody:  truncate(result.Body, 4096),
		ErrorMessage:  truncate(errorMessage(result), 1024),
		Duration:      result.Duration.Milliseconds(),
		InstanceID:    d.instanceID,
	}
	if err := d.execRepo.Record(ctx, exec); err != nil {
		if errors.Is(err, repository.ErrDuplicateExecution) {
			d.log.Warn("duplicate execution record (token existed)", zap.String("delivery_token", token))
			return
		}
		d.log.Error("record execution failed", zap.String("task_key", taskKey), zap.Error(err))
	}
}

// buildHeaders 合并用户自定义 headers 与系统注入的幂等性 headers。
// 注意：X-Idempotency-Key 使用稳定的 task_key（而非 delivery_token），
// 使消费端可对“重试/崩溃重投”做幂等去重，从根本上杜绝重复消费。
func (d *Dispatcher) buildHeaders(task model.Task, deliveryToken string, attempt int) map[string]string {
	headers := make(map[string]string)
	if task.Headers != "" && task.Headers != "{}" {
		var userHeaders map[string]string
		if err := json.Unmarshal([]byte(task.Headers), &userHeaders); err == nil {
			for k, v := range userHeaders {
				headers[k] = v
			}
		}
	}
	headers[httpclient.HeaderIdempotencyKey] = task.TaskKey
	headers[httpclient.HeaderTaskKey] = task.TaskKey
	headers[httpclient.HeaderAttempt] = fmt.Sprintf("%d", attempt)
	headers[httpclient.HeaderDeliveryToken] = deliveryToken
	return headers
}

func errorMessage(r httpclient.Result) string {
	if r.Err != nil {
		return r.Err.Error()
	}
	return fmt.Sprintf("webhook returned status %d", r.StatusCode)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "...(truncated)"
}
