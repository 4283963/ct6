package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	CodeOK            = 0
	CodeInvalidParam  = 40001
	CodeNotFound      = 40404
	CodeConflict      = 40901
	CodeInternalError = 50000
)

type Body struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func OK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Body{Code: CodeOK, Message: "ok", Data: data})
}

func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Body{Code: CodeOK, Message: "created", Data: data})
}

func Fail(c *gin.Context, httpStatus, code int, msg string) {
	c.JSON(httpStatus, Body{Code: code, Message: msg})
}

// responseRecorder 捕获 handler 写出的状态码与响应体，供幂等中间件缓存。
type responseRecorder struct {
	gin.ResponseWriter
	status int
	body   []byte
}

func newResponseRecorder(w gin.ResponseWriter) *responseRecorder {
	return &responseRecorder{ResponseWriter: w, status: http.StatusOK}
}

func (r *responseRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body = append(r.body, b...)
	return r.ResponseWriter.Write(b)
}

func (r *responseRecorder) WriteString(s string) (int, error) {
	r.body = append(r.body, s...)
	return r.ResponseWriter.WriteString(s)
}
