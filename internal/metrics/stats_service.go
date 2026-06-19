package metrics

import (
	"context"
	"time"

	"ct6/internal/model"
	"ct6/internal/repository"
)

// DeliveryMonitor 运行时可观测接口，由 Dispatcher 实现，
// StatsService 通过此接口读取队列与熔断器状态。
type DeliveryMonitor interface {
	QueueLen() int
	QueueCap() int
	CircuitOpenCount() int
}

// StatusProvider 任务状态分布提供者，由 TaskRepository 满足。
type StatusProvider interface {
	GetByStateCounts(ctx context.Context) (map[model.TaskState]int64, error)
}

// StatsService 监控统计服务：联合 Redis 实时计数器与 MySQL 历史数据。
type StatsService struct {
	counters   *DeliveryCounters
	execRepo   repository.ExecutionRepository
	statusRepo StatusProvider
	monitor    DeliveryMonitor
}

func NewStatsService(
	counters *DeliveryCounters,
	execRepo repository.ExecutionRepository,
	statusRepo StatusProvider,
	monitor DeliveryMonitor,
) *StatsService {
	return &StatsService{
		counters:   counters,
		execRepo:   execRepo,
		statusRepo: statusRepo,
		monitor:    monitor,
	}
}

// DeliveryOverview 投递总览：实时+历史
type DeliveryOverview struct {
	Today   DeliverySnapshot `json:"today"`
	History DeliverySnapshot `json:"history"`
	Queue   QueueStatus      `json:"queue"`
	Tasks   TaskStatusCounts `json:"tasks"`
	Breaker BreakerStatus    `json:"breaker"`
}

type DeliverySnapshot struct {
	Total        int64            `json:"total"`
	Success      int64            `json:"success"`
	Failure      int64            `json:"failure"`
	SuccessRate  float64          `json:"success_rate"` // 0.0 ~ 1.0
	FailureRate  float64          `json:"failure_rate"`
	ByStatusCode map[string]int64 `json:"by_status_code"`
	ByErrorType  map[string]int64 `json:"by_error_type"`
}

type QueueStatus struct {
	Length int     `json:"length"`
	Cap    int     `json:"cap"`
	Load   float64 `json:"load"`
}

type TaskStatusCounts map[string]int64

type BreakerStatus struct {
	OpenCount int `json:"open_count"`
}

// GetOverview 返回完整的监控概览。
// historyDays 指定历史回溯天数（含今天），<= 0 表示全部历史。
func (s *StatsService) GetOverview(ctx context.Context, historyDays int) (*DeliveryOverview, error) {
	todaySnap, err := s.todayFromRedis(ctx)
	if err != nil {
		// Redis 故障不应阻断整个监控接口，返回空值而非报错
		todaySnap = emptySnapshot()
	}

	var historySnap *DeliverySnapshot
	if historyDays <= 0 {
		stats, err := s.execRepo.Stats(ctx, time.Time{}, time.Time{})
		if err != nil {
			return nil, err
		}
		historySnap = toSnapshot(stats)
	} else {
		start := time.Now().AddDate(0, 0, -historyDays+1)
		start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
		stats, err := s.execRepo.Stats(ctx, start, time.Time{})
		if err != nil {
			return nil, err
		}
		historySnap = toSnapshot(stats)
	}

	taskCounts, _ := s.statusRepo.GetByStateCounts(ctx)
	taskOut := make(TaskStatusCounts, len(taskCounts))
	for k, v := range taskCounts {
		taskOut[string(k)] = v
	}

	qLen, qCap := 0, 0
	if s.monitor != nil {
		qLen = s.monitor.QueueLen()
		qCap = s.monitor.QueueCap()
	}
	queue := QueueStatus{Length: qLen, Cap: qCap}
	if qCap > 0 {
		queue.Load = float64(qLen) / float64(qCap)
	}

	breaker := BreakerStatus{}
	if s.monitor != nil {
		breaker.OpenCount = s.monitor.CircuitOpenCount()
	}

	return &DeliveryOverview{
		Today:   *todaySnap,
		History: *historySnap,
		Queue:   queue,
		Tasks:   taskOut,
		Breaker: breaker,
	}, nil
}

func (s *StatsService) todayFromRedis(ctx context.Context) (*DeliverySnapshot, error) {
	snap, err := s.counters.TodaySnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := emptySnapshot()
	out.Total = snap["total"]
	out.Success = snap["success"]
	out.Failure = snap["failure"]

	// 状态码分布
	byStatus := make(map[string]int64)
	byError := make(map[string]int64)
	for k, v := range snap {
		if len(k) > 7 && k[:7] == "status_" {
			byStatus[k[7:]] = v
		}
	}
	if v, ok := snap["error_network"]; ok && v > 0 {
		byError["network"] = v
	}
	if v, ok := snap["error_circuit"]; ok && v > 0 {
		byError["circuit"] = v
	}
	if v, ok := snap["duplicate_exec"]; ok && v > 0 {
		byError["duplicate_exec"] = v
	}

	// 计算 HTTP 错误分类
	httpFailures := out.Failure - snap["error_network"] - snap["error_circuit"] - snap["duplicate_exec"]
	if httpFailures > 0 {
		byError["http"] = httpFailures
	}
	out.ByStatusCode = byStatus
	out.ByErrorType = byError

	out.SuccessRate = rate(out.Success, out.Total)
	out.FailureRate = rate(out.Failure, out.Total)
	return out, nil
}

func toSnapshot(s *repository.DeliveryStats) *DeliverySnapshot {
	out := emptySnapshot()
	out.Total = s.Total
	out.Success = s.Success
	out.Failure = s.Failure

	byStatus := make(map[string]int64, len(s.ByStatusCode))
	for code, cnt := range s.ByStatusCode {
		byStatus[itoa(code)] = cnt
	}
	out.ByStatusCode = byStatus

	out.ByErrorType = s.ByError
	out.SuccessRate = rate(s.Success, s.Total)
	out.FailureRate = rate(s.Failure, s.Total)
	return out
}

func emptySnapshot() *DeliverySnapshot {
	return &DeliverySnapshot{
		ByStatusCode: map[string]int64{},
		ByErrorType:  map[string]int64{},
	}
}

func rate(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
