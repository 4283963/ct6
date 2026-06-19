package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"

	"ct6/internal/middleware"
	"ct6/internal/model"
	"ct6/internal/repository"
	"ct6/internal/scheduler"
)

type TaskHandler struct {
	scheduler *scheduler.Scheduler
	taskRepo  repository.TaskRepository
	execRepo  repository.ExecutionRepository
	validate  *validator.Validate
}

func NewTaskHandler(s *scheduler.Scheduler, tr repository.TaskRepository, er repository.ExecutionRepository) *TaskHandler {
	return &TaskHandler{
		scheduler: s,
		taskRepo:  tr,
		execRepo:  er,
		validate:  validator.New(),
	}
}

// taskResponse 对外任务视图，隐藏内部自增 ID。
type taskResponse struct {
	TaskKey    string          `json:"task_key"`
	WebhookURL string          `json:"webhook_url"`
	Method     string          `json:"method"`
	Headers    string          `json:"headers"`
	Payload    string          `json:"payload,omitempty"`
	MaxRetries int             `json:"max_retries"`
	Attempt    int             `json:"attempt"`
	State      model.TaskState `json:"state"`
	NextRunAt  string          `json:"next_run_at"`
	Priority   int             `json:"priority"`
	CreatedAt  string          `json:"created_at"`
	UpdatedAt  string          `json:"updated_at"`
}

func toTaskResponse(t *model.Task) taskResponse {
	return taskResponse{
		TaskKey:    t.TaskKey,
		WebhookURL: t.WebhookURL,
		Method:     t.Method,
		Headers:    t.Headers,
		Payload:    t.Payload,
		MaxRetries: t.MaxRetries,
		Attempt:    t.Attempt,
		State:      t.State,
		NextRunAt:  formatTime(t.NextRunAt),
		Priority:   t.Priority,
		CreatedAt:  formatTime(t.CreatedAt),
		UpdatedAt:  formatTime(t.UpdatedAt),
	}
}

// Register POST /api/v1/tasks
func (h *TaskHandler) Register(c *gin.Context) {
	var in model.RegisterTaskInput
	if err := c.ShouldBindJSON(&in); err != nil {
		Fail(c, http.StatusBadRequest, middleware.CodeInvalidParam, "invalid request body: "+err.Error())
		return
	}
	if err := h.validate.Struct(in); err != nil {
		Fail(c, http.StatusBadRequest, middleware.CodeInvalidParam, "validation failed: "+err.Error())
		return
	}

	task, created, err := h.scheduler.RegisterTask(c.Request.Context(), in)
	if err != nil {
		Fail(c, http.StatusBadRequest, middleware.CodeInvalidParam, err.Error())
		return
	}

	resp := toTaskResponse(task)
	if created {
		middleware.Created(c, resp)
	} else {
		middleware.OK(c, resp)
	}
}

// Get GET /api/v1/tasks/:task_key
func (h *TaskHandler) Get(c *gin.Context) {
	taskKey := c.Param("task_key")
	task, err := h.taskRepo.GetByTaskKey(c.Request.Context(), taskKey)
	if err != nil {
		if errors.Is(err, repository.ErrTaskNotFound) {
			Fail(c, http.StatusNotFound, middleware.CodeNotFound, "task not found")
			return
		}
		Fail(c, http.StatusInternalServerError, middleware.CodeInternalError, "get task failed")
		return
	}
	middleware.OK(c, toTaskResponse(task))
}

// ListExecutions GET /api/v1/tasks/:task_key/executions
func (h *TaskHandler) ListExecutions(c *gin.Context) {
	taskKey := c.Param("task_key")
	if _, err := h.taskRepo.GetByTaskKey(c.Request.Context(), taskKey); err != nil {
		if errors.Is(err, repository.ErrTaskNotFound) {
			Fail(c, http.StatusNotFound, middleware.CodeNotFound, "task not found")
			return
		}
		Fail(c, http.StatusInternalServerError, middleware.CodeInternalError, "get task failed")
		return
	}
	list, err := h.execRepo.ListByTaskKey(c.Request.Context(), taskKey)
	if err != nil {
		Fail(c, http.StatusInternalServerError, middleware.CodeInternalError, "list executions failed")
		return
	}
	middleware.OK(c, list)
}

// Stats GET /api/v1/stats
func (h *TaskHandler) Stats(c *gin.Context) {
	counts, err := h.taskRepo.GetByStateCounts(c.Request.Context())
	if err != nil {
		Fail(c, http.StatusInternalServerError, middleware.CodeInternalError, "get stats failed")
		return
	}
	middleware.OK(c, counts)
}
