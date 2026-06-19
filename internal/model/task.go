package model

import (
	"errors"
	"time"
)

// TaskState 表示任务在状态机中的当前状态。
type TaskState string

const (
	// StatePending 已注册，等待被调度分发。
	StatePending TaskState = "pending"
	// StateDispatched 已被某个 Dispatcher 实例认领并正在投递 Webhook。
	StateDispatched TaskState = "dispatched"
	// StateSucceeded Webhook 投递成功（2xx）。
	StateSucceeded TaskState = "succeeded"
	// StateFailed 投递失败，等待退避后重试。
	StateFailed TaskState = "failed"
	// StateDead 重试次数耗尽，进入死信状态。
	StateDead TaskState = "dead"
)

// IsValid 判断状态值是否合法。
func (s TaskState) IsValid() bool {
	switch s {
	case StatePending, StateDispatched, StateSucceeded, StateFailed, StateDead:
		return true
	}
	return false
}

// Task 调度任务实体，对应 tasks 表。
type Task struct {
	ID         uint      `gorm:"primaryKey;autoIncrement"`
	TaskKey    string    `gorm:"column:task_key;uniqueIndex;size:128;not null;comment:业务幂等键"`
	WebhookURL string    `gorm:"column:webhook_url;size:512;not null"`
	Method     string    `gorm:"column:method;size:16;not null;default:POST"`
	Headers    string    `gorm:"column:headers;type:json"`
	Payload    string    `gorm:"column:payload;type:longtext"`
	MaxRetries int       `gorm:"column:max_retries;not null;default:5"`
	Attempt    int       `gorm:"column:attempt;not null;default:0"`
	State      TaskState `gorm:"column:state;size:16;not null;default:pending;index"`
	NextRunAt  time.Time `gorm:"column:next_run_at;not null;index:idx_dispatch"`
	Priority   int       `gorm:"column:priority;not null;default:0;index:idx_dispatch"`
	CreatedAt  time.Time `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt  time.Time `gorm:"column:updated_at;autoUpdateTime"`
}

func (Task) TableName() string { return "tasks" }

// TaskExecution 单次投递执行记录，用于审计与排查重复分发。
type TaskExecution struct {
	ID            uint      `gorm:"primaryKey;autoIncrement"`
	TaskKey       string    `gorm:"column:task_key;size:128;not null;index:idx_task_exec"`
	Attempt       int       `gorm:"column:attempt;not null;index:idx_task_exec"`
	DeliveryToken string    `gorm:"column:delivery_token;size:64;not null;uniqueIndex"`
	StatusCode    int       `gorm:"column:status_code"`
	ResponseBody  string    `gorm:"column:response_body;type:text"`
	ErrorMessage  string    `gorm:"column:error_message;type:text"`
	Duration      int64     `gorm:"column:duration_ms"`
	InstanceID    string    `gorm:"column:instance_id;size:64"`
	CreatedAt     time.Time `gorm:"column:created_at;autoCreateTime"`
}

func (TaskExecution) TableName() string { return "task_executions" }

// RegisterTaskInput 注册任务时的入参。
type RegisterTaskInput struct {
	TaskKey    string            `json:"task_key" binding:"required,max=128"`
	WebhookURL string            `json:"webhook_url" binding:"required,url"`
	Method     string            `json:"method" binding:"omitempty,oneof=GET POST PUT PATCH DELETE"`
	Headers    map[string]string `json:"headers"`
	Payload    string            `json:"payload"`
	MaxRetries int               `json:"max_retries"`
	Priority   int               `json:"priority"`
}

// TaskStateError 状态机非法迁移错误。
var ErrInvalidTransition = errors.New("invalid task state transition")

// AllowedTransitions 描述状态机允许的迁移关系，用于在 Repository 层做校验。
var AllowedTransitions = map[TaskState][]TaskState{
	StatePending:    {StateDispatched, StateSucceeded, StateDead},
	StateDispatched: {StateSucceeded, StateFailed, StateDead},
	StateFailed:     {StatePending, StateDead},
	StateSucceeded:  {},
	StateDead:       {},
}

// CanTransition 校验 from -> to 是否允许。
func CanTransition(from, to TaskState) bool {
	for _, s := range AllowedTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}
