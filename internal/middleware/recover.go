package middleware

import (
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"ct6/pkg/logger"
)

// Recover 捕获 panic，防止单个请求崩溃拖垮整个进程。
func Recover() gin.HandlerFunc {
	log := logger.L().Named("recover")
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic recovered",
					zap.Any("error", rec),
					zap.String("request_id", c.GetString(CtxRequestID)),
					zap.ByteString("stack", debug.Stack()),
				)
				if !c.Writer.Written() {
					Fail(c, http.StatusInternalServerError, CodeInternalError, "internal server error")
				}
				c.Abort()
			}
		}()
		c.Next()
	}
}
