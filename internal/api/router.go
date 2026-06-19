package api

import (
	"github.com/gin-gonic/gin"

	"ct6/internal/api/handler"
	"ct6/internal/config"
	"ct6/internal/middleware"
)

// NewRouter 组装 Gin 引擎：全局中间件 + 路由分组。
func NewRouter(
	cfg *config.Config,
	taskH *handler.TaskHandler,
	healthH *handler.HealthHandler,
	idemp *middleware.Idempotency,
) *gin.Engine {
	if cfg.App.Environment == "prod" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(
		middleware.RequestID(),
		middleware.Recover(),
		middleware.Logger(),
	)
	r.RedirectTrailingSlash = false
	r.HandleMethodNotAllowed = true

	r.GET("/healthz", healthH.Live)
	r.GET("/readyz", healthH.Ready)

	v1 := r.Group("/api/v1")
	{
		// 仅对写操作启用 HTTP 幂等中间件。
		tasks := v1.Group("/tasks")
		tasks.Use(idemp.Middleware())
		{
			tasks.POST("", taskH.Register)
		}
		// 只读接口无需幂等中间件。
		ro := v1.Group("/tasks")
		{
			ro.GET("/:task_key", taskH.Get)
			ro.GET("/:task_key/executions", taskH.ListExecutions)
		}
		v1.GET("/stats", taskH.Stats)
	}

	return r
}
