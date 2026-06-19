package repository

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ct6/internal/model"
)

var ErrDuplicateExecution = errors.New("duplicate execution for delivery token")

// DeliveryStats 投递统计聚合结果。
type DeliveryStats struct {
	Total        int64
	Success      int64
	Failure      int64
	ByStatusCode map[int]int64
	ByError      map[string]int64
}

// ExecutionRepository 单次投递执行记录持久化接口。
type ExecutionRepository interface {
	// Record 幂等记录一次执行：以 delivery_token 唯一索引防止重复写入。
	Record(ctx context.Context, e *model.TaskExecution) error
	ListByTaskKey(ctx context.Context, taskKey string) ([]model.TaskExecution, error)
	// Stats 统计指定时间范围内的投递数据。start/end 可为零值表示不限。
	Stats(ctx context.Context, start, end time.Time) (*DeliveryStats, error)
}

type executionRepo struct {
	db *gorm.DB
}

func NewExecutionRepository(db *gorm.DB) ExecutionRepository {
	return &executionRepo{db: db}
}

// Record 写入执行记录。若 delivery_token 已存在则忽略（DoNothing），
// 这是“绝对不重复消费”的审计层兜底：同一个 task_key+attempt 只会有一条记录。
func (r *executionRepo) Record(ctx context.Context, e *model.TaskExecution) error {
	tx := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "delivery_token"}}, DoNothing: true}).
		Create(e)
	if err := tx.Error; err != nil {
		return err
	}
	if tx.RowsAffected == 0 {
		return ErrDuplicateExecution
	}
	return nil
}

func (r *executionRepo) ListByTaskKey(ctx context.Context, taskKey string) ([]model.TaskExecution, error) {
	var list []model.TaskExecution
	err := r.db.WithContext(ctx).
		Where("task_key = ?", taskKey).
		Order("attempt ASC").
		Find(&list).Error
	return list, err
}

// Stats 统计指定时间窗口内的投递数据。
// 利用一次 GROUP BY status_code 聚合出 total / success / failure / 状态码分布，
// 再从错误信息中粗略归类网络错误/HTTP 错误比例。
func (r *executionRepo) Stats(ctx context.Context, start, end time.Time) (*DeliveryStats, error) {
	type row struct {
		StatusCode int
		Count      int64
	}
	var rows []row

	q := r.db.WithContext(ctx).
		Model(&model.TaskExecution{}).
		Select("status_code, count(*) as count").
		Group("status_code").
		Order("status_code ASC")
	if !start.IsZero() {
		q = q.Where("created_at >= ?", start)
	}
	if !end.IsZero() {
		q = q.Where("created_at < ?", end)
	}
	if err := q.Scan(&rows).Error; err != nil {
		return nil, err
	}

	stats := &DeliveryStats{
		ByStatusCode: make(map[int]int64, len(rows)),
		ByError:      make(map[string]int64),
	}
	for _, r := range rows {
		stats.Total += r.Count
		stats.ByStatusCode[r.StatusCode] = r.Count
		if r.StatusCode >= 200 && r.StatusCode < 300 {
			stats.Success += r.Count
			stats.ByError["success"] += r.Count
		} else if r.StatusCode == 0 {
			// status_code 为 0 表示客户端侧错误（网络错误/熔断/超时）
			stats.Failure += r.Count
			stats.ByError["network"] += r.Count
		} else if r.StatusCode >= 400 && r.StatusCode < 500 {
			stats.Failure += r.Count
			stats.ByError["http_4xx"] += r.Count
		} else if r.StatusCode >= 500 {
			stats.Failure += r.Count
			stats.ByError["http_5xx"] += r.Count
		} else {
			stats.Failure += r.Count
			stats.ByError["http_other"] += r.Count
		}
	}
	return stats, nil
}
