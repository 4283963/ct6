package lock

import (
	"context"
	"errors"
	"time"
)

// 锁相关错误。
var (
	ErrLockNotAcquired = errors.New("lock: not acquired")
	ErrLockNotHeld     = errors.New("lock: not held by current owner")
)

// Locker 分布式锁抽象。
// 返回的 Release 必须在业务结束后调用以释放锁；若上下文超时或释放失败应记录日志。
type Locker interface {
	// Acquire 尝试获取锁，失败返回 ErrLockNotAcquired。
	Acquire(ctx context.Context, key, token string, ttl time.Duration) (Release, error)
	// AcquireWithRetry 在 timeout 内自旋重试获取锁。
	AcquireWithRetry(ctx context.Context, key, token string, ttl, retryInterval, timeout time.Duration) (Release, error)
}

// Release 释放锁的函数。返回是否成功释放（false 表示锁已过期或属于他人）。
type Release func(ctx context.Context) (bool, error)
