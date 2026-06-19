package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// 解锁 Lua 脚本：仅当 token 一致才删除，避免误删他人持有的锁。
const unlockScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`

// 续期 Lua 脚本：仅当 token 一致才刷新过期时间。
const renewScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end`

type redisLocker struct {
	client    *redis.Client
	namespace string
}

// NewRedisLocker 创建基于 Redis 的分布式锁实现。
func NewRedisLocker(client *redis.Client, namespace string) Locker {
	if namespace == "" {
		namespace = "lock"
	}
	return &redisLocker{client: client, namespace: namespace}
}

func (l *redisLocker) key(k string) string {
	return fmt.Sprintf("%s:%s", l.namespace, k)
}

// Acquire 使用 SET key value NX PX ttl 原子获取锁。
func (l *redisLocker) Acquire(ctx context.Context, key, token string, ttl time.Duration) (Release, error) {
	k := l.key(key)
	ok, err := l.client.SetNX(ctx, k, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("redis SetNX: %w", err)
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}
	return func(ctx context.Context) (bool, error) {
		res, err := l.client.Eval(ctx, unlockScript, []string{k}, token).Int64()
		if err != nil {
			return false, fmt.Errorf("redis eval unlock: %w", err)
		}
		return res == 1, nil
	}, nil
}

// AcquireWithRetry 在 timeout 内以指数退避的间隔自旋获取锁。
// 指数退避间隔可避免 thundering herd：大量任务在同一时刻反复打 Redis。
// 间隔序列约为 baseInterval, 2*baseInterval, 4*baseInterval ...  capped at 500ms。
func (l *redisLocker) AcquireWithRetry(ctx context.Context, key, token string, ttl, baseInterval, timeout time.Duration) (Release, error) {
	const maxInterval = 500 * time.Millisecond
	deadline := time.Now().Add(timeout)
	interval := baseInterval
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	for {
		rel, err := l.Acquire(ctx, key, token, ttl)
		if err == nil {
			return rel, nil
		}
		if !errors.Is(err, ErrLockNotAcquired) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, ErrLockNotAcquired
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
		// 指数增长，带封顶
		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
	}
}

// Renew 续期锁（可选），仅持有者可续。
func (l *redisLocker) Renew(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	k := l.key(key)
	res, err := l.client.Eval(ctx, renewScript, []string{k}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("redis eval renew: %w", err)
	}
	return res == 1, nil
}
