package metrics

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// DeliveryCounters Redis 实时投递计数器。
// 使用单个 hash 存储，key = {namespace}:counters:delivery:{yyyy-mm-dd}
// 字段：
//
//	total             总投递次数（含重试）
//	success           成功次数（2xx）
//	failure           失败次数（非 2xx 或网络错误）
//	status_200 ...    各 HTTP 状态码计数
//	error_network     网络/超时错误数
//	error_circuit     熔断器快速失败数
//	duplicate_exec    执行记录重复写入数（表示重复分发被拦截）
type DeliveryCounters struct {
	rdb       *redis.Client
	namespace string
}

func NewDeliveryCounters(rdb *redis.Client, namespace string) *DeliveryCounters {
	if namespace == "" {
		namespace = "td"
	}
	return &DeliveryCounters{rdb: rdb, namespace: namespace}
}

func (c *DeliveryCounters) key(day time.Time) string {
	return fmt.Sprintf("%s:metrics:delivery:%s", c.namespace, day.UTC().Format("2006-01-02"))
}

// IncSuccess 记录一次成功投递。
func (c *DeliveryCounters) IncSuccess(ctx context.Context, statusCode int) {
	k := c.key(time.Now())
	pipe := c.rdb.Pipeline()
	pipe.HIncrBy(ctx, k, "total", 1)
	pipe.HIncrBy(ctx, k, "success", 1)
	pipe.HIncrBy(ctx, k, fmt.Sprintf("status_%d", statusCode), 1)
	pipe.Expire(ctx, k, 30*24*time.Hour) // 保留 30 天
	_, _ = pipe.Exec(ctx)
}

// IncFailure 记录一次失败投递。reason: "network" / "circuit" / "http"
func (c *DeliveryCounters) IncFailure(ctx context.Context, statusCode int, reason string) {
	k := c.key(time.Now())
	pipe := c.rdb.Pipeline()
	pipe.HIncrBy(ctx, k, "total", 1)
	pipe.HIncrBy(ctx, k, "failure", 1)
	if statusCode > 0 {
		pipe.HIncrBy(ctx, k, fmt.Sprintf("status_%d", statusCode), 1)
	}
	switch reason {
	case "network":
		pipe.HIncrBy(ctx, k, "error_network", 1)
	case "circuit":
		pipe.HIncrBy(ctx, k, "error_circuit", 1)
	}
	pipe.Expire(ctx, k, 30*24*time.Hour)
	_, _ = pipe.Exec(ctx)
}

// IncDuplicate 记录重复执行拦截（幂等生效次数）。
func (c *DeliveryCounters) IncDuplicate(ctx context.Context) {
	k := c.key(time.Now())
	pipe := c.rdb.Pipeline()
	pipe.HIncrBy(ctx, k, "total", 1)
	pipe.HIncrBy(ctx, k, "duplicate_exec", 1)
	pipe.Expire(ctx, k, 30*24*time.Hour)
	_, _ = pipe.Exec(ctx)
}

// TodaySnapshot 读取当日快照。返回 map 形式，所有 value 都是 int64。
func (c *DeliveryCounters) TodaySnapshot(ctx context.Context) (map[string]int64, error) {
	return c.Snapshot(ctx, time.Now())
}

// Snapshot 读取指定日期的快照。
func (c *DeliveryCounters) Snapshot(ctx context.Context, day time.Time) (map[string]int64, error) {
	k := c.key(day)
	m, err := c.rdb.HGetAll(ctx, k).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGETALL %s: %w", k, err)
	}
	result := make(map[string]int64, len(m))
	for field, val := range m {
		n, parseErr := strconv.ParseInt(val, 10, 64)
		if parseErr != nil {
			continue
		}
		result[field] = n
	}
	return result, nil
}

// Range 读取连续多日快照并汇总。end 含当天。
func (c *DeliveryCounters) Range(ctx context.Context, start, end time.Time) (map[string]int64, error) {
	start = start.UTC()
	end = end.UTC()
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, time.UTC)

	total := make(map[string]int64)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		snap, err := c.Snapshot(ctx, d)
		if err != nil {
			return nil, err
		}
		for k, v := range snap {
			total[k] += v
		}
	}
	return total, nil
}
