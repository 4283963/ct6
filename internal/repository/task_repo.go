package repository

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ct6/internal/model"
)

// 预定义错误，供上层区分业务语义。
var (
	ErrTaskNotFound = errors.New("task not found")
	ErrNotClaimed   = errors.New("task was not claimed (already dispatched or state changed)")
)

// TaskRepository 任务持久化接口，便于上层依赖注入与单测替身。
type TaskRepository interface {
	Register(ctx context.Context, t *model.Task) (existing *model.Task, created bool, err error)
	GetByTaskKey(ctx context.Context, taskKey string) (*model.Task, error)
	FetchDispatchable(ctx context.Context, now time.Time, batchSize int) ([]model.Task, error)
	// FetchPending / FetchFailed 分别拉取新任务与重试任务，供 Scheduler 做优先级配比。
	FetchPending(ctx context.Context, now time.Time, limit int) ([]model.Task, error)
	FetchFailed(ctx context.Context, now time.Time, limit int) ([]model.Task, error)
	ClaimForDispatch(ctx context.Context, taskKey string, fromStates []model.TaskState) error
	MarkSucceeded(ctx context.Context, taskKey string) error
	MarkFailed(ctx context.Context, taskKey string, attempt int, nextRunAt time.Time, errMsg string) error
	MarkDead(ctx context.Context, taskKey string, attempt int, errMsg string) error
	// ResetStuckDispatched 将长期停留在 dispatched 状态（疑似实例崩溃）的任务
	// 重置为 failed 并安排立即重试。返回受影响行数。依赖消费端对幂等键去重，
	// 因此即使造成“重复分发”也不会导致“重复消费”。
	ResetStuckDispatched(ctx context.Context, olderThan time.Time, nextRunAt time.Time) (int64, error)
	GetByStateCounts(ctx context.Context) (map[model.TaskState]int64, error)
}

type taskRepo struct {
	db *gorm.DB
}

func NewTaskRepository(db *gorm.DB) TaskRepository {
	return &taskRepo{db: db}
}

// Register 幂等注册：以 task_key 为唯一键。
// 若已存在则返回既有任务（created=false），保证重复注册不会产生重复任务。
func (r *taskRepo) Register(ctx context.Context, t *model.Task) (*model.Task, bool, error) {
	// 使用 ON CONFLICT 语义：MySQL 下 clause.OnConflict 会生成 INSERT ... ON DUPLICATE KEY UPDATE。
	// 这里用 DoNothing 确保已存在时不覆盖任何字段，由后续 First 还原最新值。
	tx := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "task_key"}}, DoNothing: true}).
		Create(t)
	if err := tx.Error; err != nil {
		return nil, false, err
	}
	created := tx.RowsAffected == 1
	if created {
		return t, true, nil
	}

	var existing model.Task
	if err := r.db.WithContext(ctx).Where("task_key = ?", t.TaskKey).First(&existing).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, false, ErrTaskNotFound
		}
		return nil, false, err
	}
	return &existing, false, nil
}

func (r *taskRepo) GetByTaskKey(ctx context.Context, taskKey string) (*model.Task, error) {
	var t model.Task
	if err := r.db.WithContext(ctx).Where("task_key = ?", taskKey).First(&t).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}
	return &t, nil
}

// FetchDispatchable 拉取可分发候选：处于 pending/failed 且已到达投递时间。
// 保留该方法用于只读统计/兼容场景；调度主循环应使用 FetchPending / FetchFailed 精确配比。
func (r *taskRepo) FetchDispatchable(ctx context.Context, now time.Time, batchSize int) ([]model.Task, error) {
	var tasks []model.Task
	err := r.db.WithContext(ctx).
		Where("state IN ? AND next_run_at <= ?", []model.TaskState{model.StatePending, model.StateFailed}, now).
		Order("priority DESC, next_run_at ASC").
		Limit(batchSize).
		Find(&tasks).Error
	return tasks, err
}

// FetchPending 拉取 PENDING 新任务（高优先级），保证新任务不会被重试任务饿死。
func (r *taskRepo) FetchPending(ctx context.Context, now time.Time, limit int) ([]model.Task, error) {
	var tasks []model.Task
	err := r.db.WithContext(ctx).
		Where("state = ? AND next_run_at <= ?", model.StatePending, now).
		Order("priority DESC, next_run_at ASC").
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

// FetchFailed 拉取 FAILED 重试任务（低优先级）。
func (r *taskRepo) FetchFailed(ctx context.Context, now time.Time, limit int) ([]model.Task, error) {
	var tasks []model.Task
	err := r.db.WithContext(ctx).
		Where("state = ? AND next_run_at <= ?", model.StateFailed, now).
		Order("priority DESC, next_run_at ASC").
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

// ClaimForDispatch 原子地将任务从 pending/failed 迁移到 dispatched。
// 这是“绝对不重复分发”的数据库级权威幂等屏障：
// 多个实例并发执行时，仅当 state 仍处于 fromStates 才能更新成功。
func (r *taskRepo) ClaimForDispatch(ctx context.Context, taskKey string, fromStates []model.TaskState) error {
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("task_key = ? AND state IN ?", taskKey, fromStates).
		Update("state", model.StateDispatched)
	if err := res.Error; err != nil {
		return err
	}
	if res.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// MarkSucceeded 标记任务成功。仅 dispatched -> succeeded 合法。
func (r *taskRepo) MarkSucceeded(ctx context.Context, taskKey string) error {
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("task_key = ? AND state = ?", taskKey, model.StateDispatched).
		Update("state", model.StateSucceeded)
	if err := res.Error; err != nil {
		return err
	}
	if res.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// MarkFailed 标记失败并设置下一次重试时间，回退到 failed 状态。
func (r *taskRepo) MarkFailed(ctx context.Context, taskKey string, attempt int, nextRunAt time.Time, errMsg string) error {
	updates := map[string]interface{}{
		"state":       model.StateFailed,
		"attempt":     attempt,
		"next_run_at": nextRunAt,
	}
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("task_key = ? AND state = ?", taskKey, model.StateDispatched).
		Updates(updates)
	if err := res.Error; err != nil {
		return err
	}
	if res.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// MarkDead 重试耗尽，进入死信。
func (r *taskRepo) MarkDead(ctx context.Context, taskKey string, attempt int, errMsg string) error {
	updates := map[string]interface{}{
		"state":   model.StateDead,
		"attempt": attempt,
	}
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("task_key = ? AND state = ?", taskKey, model.StateDispatched).
		Updates(updates)
	if err := res.Error; err != nil {
		return err
	}
	if res.RowsAffected == 0 {
		return ErrNotClaimed
	}
	return nil
}

// ResetStuckDispatched 将 updated_at < olderThan 且仍处于 dispatched 的任务重置为 failed。
// 这是崩溃恢复机制：避免因实例宕机导致任务永久卡在 dispatched。
func (r *taskRepo) ResetStuckDispatched(ctx context.Context, olderThan time.Time, nextRunAt time.Time) (int64, error) {
	updates := map[string]interface{}{
		"state":       model.StateFailed,
		"next_run_at": nextRunAt,
	}
	res := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Where("state = ? AND updated_at < ?", model.StateDispatched, olderThan).
		Updates(updates)
	if err := res.Error; err != nil {
		return 0, err
	}
	return res.RowsAffected, nil
}

func (r *taskRepo) GetByStateCounts(ctx context.Context) (map[model.TaskState]int64, error) {
	type result struct {
		State model.TaskState
		Count int64
	}
	var results []result
	err := r.db.WithContext(ctx).
		Model(&model.Task{}).
		Select("state, count(*) as count").
		Group("state").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}
	m := make(map[model.TaskState]int64, len(results))
	for _, r := range results {
		m[r.State] = r.Count
	}
	return m, nil
}
