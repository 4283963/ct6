package repository

import (
	"context"
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"ct6/internal/model"
)

var ErrDuplicateExecution = errors.New("duplicate execution for delivery token")

// ExecutionRepository 单次投递执行记录持久化接口。
type ExecutionRepository interface {
	// Record 幂等记录一次执行：以 delivery_token 唯一索引防止重复写入。
	Record(ctx context.Context, e *model.TaskExecution) error
	ListByTaskKey(ctx context.Context, taskKey string) ([]model.TaskExecution, error)
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
