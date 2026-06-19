package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"ct6/pkg/logger"
)

const (
	HeaderIdempotencyKey = "Idempotency-Key"
	idempPrefix          = "idemp:"
	idempMarker          = "processing"
)

type cachedResponse struct {
	Status      int    `json:"status"`
	ContentType string `json:"content_type"`
	Body        string `json:"body"`
}

// Idempotency 基于 Redis 的 HTTP 请求幂等中间件。
// 当非 GET 请求携带 Idempotency-Key 时：
//   - 命中已缓存响应 -> 原样回放，避免重复执行副作用
//   - 命中处理中标记(processing) -> 返回 409，防止并发重复处理
//   - 否则占位处理并缓存最终响应（5xx 不缓存，允许重试）
type Idempotency struct {
	rdb       *redis.Client
	ttl       time.Duration
	markerTTL time.Duration
}

func NewIdempotency(rdb *redis.Client, ttl time.Duration) *Idempotency {
	marker := ttl
	if marker > 30*time.Second {
		marker = 30 * time.Second
	}
	return &Idempotency{rdb: rdb, ttl: ttl, markerTTL: marker}
}

func (m *Idempotency) Middleware() gin.HandlerFunc {
	log := logger.L().Named("idempotency")
	return func(c *gin.Context) {
		if !shouldIntercept(c) {
			c.Next()
			return
		}
		key := idempPrefix + c.GetHeader(HeaderIdempotencyKey)
		ctx := c.Request.Context()

		// 1. 尝试命中已缓存的最终响应。
		if cached, ok := m.loadResponse(ctx, key); ok {
			m.writeCached(c, cached)
			log.Info("idempotency cache hit", zap.String("key", key), zap.Int("status", cached.Status))
			return
		}

		// 2. 占位标记，防止并发重复处理。
		ok, err := m.rdb.SetNX(ctx, key, idempMarker, m.markerTTL).Result()
		if err != nil {
			log.Warn("idempotency setnx failed, fallback to passthrough", zap.Error(err))
			c.Next()
			return
		}
		if !ok {
			// 已有相同 key 在处理中。
			Fail(c, http.StatusConflict, CodeConflict, "request with same idempotency-key is being processed")
			c.Abort()
			return
		}

		// 3. 记录响应并按需缓存。
		rec := newResponseRecorder(c.Writer)
		c.Writer = rec
		c.Next()

		if rec.status >= 500 {
			// 服务端错误不缓存，删除标记以便客户端重试。
			if err := m.rdb.Del(ctx, key).Err(); err != nil {
				log.Warn("idempotency delete marker failed", zap.Error(err))
			}
			return
		}

		resp := cachedResponse{
			Status:      rec.status,
			ContentType: rec.Header().Get("Content-Type"),
			Body:        string(rec.body),
		}
		if err := m.saveResponse(ctx, key, resp); err != nil {
			log.Warn("idempotency cache save failed", zap.Error(err))
		}
	}
}

func shouldIntercept(c *gin.Context) bool {
	switch c.Request.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return c.GetHeader(HeaderIdempotencyKey) != ""
	default:
		return false
	}
}

func (m *Idempotency) loadResponse(ctx context.Context, key string) (cachedResponse, bool) {
	var cached cachedResponse
	val, err := m.rdb.Get(ctx, key).Result()
	if err != nil {
		return cached, false
	}
	if val == idempMarker {
		// 命中标记而非最终响应：返回 not-ok，触发 409 分支。
		return cached, false
	}
	if err := json.Unmarshal([]byte(val), &cached); err != nil {
		return cached, false
	}
	return cached, true
}

func (m *Idempotency) saveResponse(ctx context.Context, key string, resp cachedResponse) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return m.rdb.Set(ctx, key, b, m.ttl).Err()
}

func (m *Idempotency) writeCached(c *gin.Context, resp cachedResponse) {
	if resp.ContentType != "" {
		c.Writer.Header().Set("Content-Type", resp.ContentType)
	}
	c.Writer.Header().Set("X-Idempotent-Replay", "true")
	c.Writer.WriteHeader(resp.Status)
	_, _ = c.Writer.WriteString(resp.Body)
}
