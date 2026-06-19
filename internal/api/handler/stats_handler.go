package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"ct6/internal/metrics"
	"ct6/internal/middleware"
)

type StatsHandler struct {
	svc *metrics.StatsService
}

func NewStatsHandler(svc *metrics.StatsService) *StatsHandler {
	return &StatsHandler{svc: svc}
}

// Overview GET /api/v1/stats/overview
// Query 参数：
//
//	days - 历史统计天数（默认 7，0 表示全部历史）
func (h *StatsHandler) Overview(c *gin.Context) {
	days := 7
	if raw := c.Query("days"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			days = n
		}
	}

	ov, err := h.svc.GetOverview(c.Request.Context(), days)
	if err != nil {
		Fail(c, http.StatusInternalServerError, middleware.CodeInternalError, "get stats failed: "+err.Error())
		return
	}
	middleware.OK(c, ov)
}
