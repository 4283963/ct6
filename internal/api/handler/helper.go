package handler

import (
	"time"

	"github.com/gin-gonic/gin"

	"ct6/internal/middleware"
)

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func Fail(c *gin.Context, httpStatus, code int, msg string) {
	middleware.Fail(c, httpStatus, code, msg)
}
