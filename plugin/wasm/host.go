package wasm

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	extism "github.com/extism/go-sdk"
	"github.com/florinutz/pgcdc/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// httpRequest is the JSON structure plugins send for outbound HTTP.
type httpRequest struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// httpResponse is the JSON structure returned to plugins.
type httpResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// HostFunctions returns the set of host functions available to all plugins.
func HostFunctions(logger *slog.Logger, pluginName string) []extism.HostFunction {
	return []extism.HostFunction{
		logHostFn(logger),
		metricIncHostFn(pluginName),
		httpRequestHostFn(logger),
	}
}

// logHostFn provides pgcdc_log(level_ptr, msg_ptr) to plugins.
func logHostFn(logger *slog.Logger) extism.HostFunction {
	fn := extism.NewHostFunctionWithStack(
		"pgcdc_log",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			level, err := p.ReadString(stack[0])
			if err != nil {
				return
			}
			msg, err := p.ReadString(stack[1])
			if err != nil {
				return
			}

			switch level {
			case "debug":
				logger.Debug(msg, "source", "plugin")
			case "info":
				logger.Info(msg, "source", "plugin")
			case "warn":
				logger.Warn(msg, "source", "plugin")
			case "error":
				logger.Error(msg, "source", "plugin")
			default:
				logger.Info(msg, "source", "plugin", "level", level)
			}
		},
		[]extism.ValueType{extism.ValueTypePTR, extism.ValueTypePTR},
		nil,
	)
	fn.SetNamespace("pgcdc")
	return fn
}

// metricIncHostFn provides pgcdc_metric_inc(name_ptr, value_f64) to plugins.
func metricIncHostFn(pluginName string) extism.HostFunction {
	// Lazily create custom counters as plugins request them.
	customCounters := make(map[string]prometheus.Counter)

	fn := extism.NewHostFunctionWithStack(
		"pgcdc_metric_inc",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			name, err := p.ReadString(stack[0])
			if err != nil {
				return
			}
			value := extism.DecodeF64(stack[1])

			counter, ok := customCounters[name]
			if !ok {
				counter = prometheus.NewCounter(prometheus.CounterOpts{
					Name:        "pgcdc_plugin_custom_" + name,
					Help:        "Custom plugin metric: " + name,
					ConstLabels: prometheus.Labels{"plugin": pluginName},
				})
				_ = prometheus.Register(counter)
				customCounters[name] = counter
			}

			counter.Add(value)
			metrics.PluginCalls.WithLabelValues(pluginName, "metric_inc").Inc()
		},
		[]extism.ValueType{extism.ValueTypePTR, extism.ValueTypeF64},
		nil,
	)
	fn.SetNamespace("pgcdc")
	return fn
}

// httpRequestHostFn provides pgcdc_http_request(req_ptr) -> resp_ptr to plugins.
func httpRequestHostFn(logger *slog.Logger) extism.HostFunction {
	client := &http.Client{Timeout: 30 * time.Second}

	fn := extism.NewHostFunctionWithStack(
		"pgcdc_http_request",
		func(ctx context.Context, p *extism.CurrentPlugin, stack []uint64) {
			reqBytes, err := p.ReadBytes(stack[0])
			if err != nil {
				writeError(p, stack, "read request: "+err.Error())
				return
			}

			var req httpRequest
			if err := json.Unmarshal(reqBytes, &req); err != nil {
				writeError(p, stack, "unmarshal request: "+err.Error())
				return
			}

			if req.Method == "" {
				req.Method = "GET"
			}

			var bodyReader io.Reader
			if req.Body != "" {
				bodyReader = bytes.NewBufferString(req.Body)
			}

			httpReq, err := http.NewRequestWithContext(ctx, req.Method, req.URL, bodyReader)
			if err != nil {
				writeError(p, stack, "create request: "+err.Error())
				return
			}
			for k, v := range req.Headers {
				httpReq.Header.Set(k, v)
			}

			resp, err := client.Do(httpReq)
			if err != nil {
				writeError(p, stack, "http request: "+err.Error())
				return
			}
			defer func() { _ = resp.Body.Close() }()

			body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB limit
			if err != nil {
				writeError(p, stack, "read response: "+err.Error())
				return
			}

			headers := make(map[string]string)
			for k := range resp.Header {
				headers[k] = resp.Header.Get(k)
			}

			result := httpResponse{
				Status:  resp.StatusCode,
				Headers: headers,
				Body:    string(body),
			}
			resultBytes, _ := json.Marshal(result)
			offset, err := p.WriteBytes(resultBytes)
			if err != nil {
				logger.Error("write http response to plugin", "error", err)
				return
			}
			stack[0] = offset
		},
		[]extism.ValueType{extism.ValueTypePTR},
		[]extism.ValueType{extism.ValueTypePTR},
	)
	fn.SetNamespace("pgcdc")
	return fn
}

func writeError(p *extism.CurrentPlugin, stack []uint64, msg string) {
	resp := httpResponse{Status: 0, Body: msg}
	b, _ := json.Marshal(resp)
	offset, err := p.WriteBytes(b)
	if err != nil {
		return
	}
	stack[0] = offset
}
