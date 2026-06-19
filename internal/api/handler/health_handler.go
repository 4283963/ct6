package handler

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"ct6/internal/middleware"
)

type HealthHandler struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewHealthHandler(db *gorm.DB, rdb *redis.Client) *HealthHandler {
	return &HealthHandler{db: db, rdb: rdb}
}

// Live GET /healthz 进程存活探针（无外部依赖）。
func (h *HealthHandler) Live(c *gin.Context) {
	middleware.OK(c, gin.H{"status": "alive"})
}

// Ready GET /readyz 就绪探针：校验 MySQL 与 Redis 连通性。
func (h *HealthHandler) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	healthy := true
	details := gin.H{}

	if sqlDB, err := h.db.DB(); err == nil {
		if err := sqlDB.PingContext(ctx); err != nil {
			healthy = false
			details["mysql"] = err.Error()
		} else {
			details["mysql"] = "ok"
		}
	} else {
		healthy = false
		details["mysql"] = err.Error()
	}

	if err := h.rdb.Ping(ctx).Err(); err != nil {
		healthy = false
		details["redis"] = err.Error()
	} else {
		details["redis"] = "ok"
	}

	if !healthy {
		middleware.Fail(c, http.StatusServiceUnavailable, middleware.CodeInternalError, "not ready")
		return
	}
	middleware.OK(c, details)
}
