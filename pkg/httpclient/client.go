package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	HeaderIdempotencyKey = "X-Idempotency-Key"
	HeaderTaskKey        = "X-Task-Key"
	HeaderAttempt        = "X-Attempt"
	HeaderDeliveryToken  = "X-Delivery-Token"
	HeaderUserAgent      = "User-Agent"
)

// Result 单次 HTTP 投递结果。
type Result struct {
	StatusCode int
	Body       string
	Duration   time.Duration
	Err        error
}

// Client 用于 Webhook 投递的 HTTP 客户端，封装超时与重试无关的纯发送逻辑。
type Client struct {
	http *http.Client
}

func New(timeout time.Duration) *Client {
	return &Client{
		http: &http.Client{
			Timeout: timeout,
			// 不自动跟随重定向：Webhook 端点应稳定，跟随可能带来副作用。
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// Do 发送请求并返回结果。2xx 视为成功，其余由上层判定是否重试。
func (c *Client) Do(ctx context.Context, method, url string, headers map[string]string, payload string) Result {
	var body io.Reader
	if payload != "" {
		body = bytes.NewReader([]byte(payload))
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return Result{Err: fmt.Errorf("build request: %w", err)}
	}
	if payload != "" {
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	req.Header.Set(HeaderUserAgent, "task-dispatcher/1.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	start := time.Now()
	resp, err := c.http.Do(req)
	dur := time.Since(start)
	if err != nil {
		return Result{Err: fmt.Errorf("http do: %w", err), Duration: dur}
	}
	defer resp.Body.Close()

	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 限制 1MB，防止恶意大响应
	if readErr != nil {
		return Result{StatusCode: resp.StatusCode, Err: fmt.Errorf("read body: %w", readErr), Duration: dur}
	}
	return Result{StatusCode: resp.StatusCode, Body: string(raw), Duration: dur}
}

// IsSuccess 判断状态码是否视为投递成功。
func (r Result) IsSuccess() bool {
	return r.Err == nil && r.StatusCode >= 200 && r.StatusCode < 300
}
