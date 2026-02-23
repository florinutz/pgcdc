package config

import "time"

type Config struct {
	DatabaseURL     string         `mapstructure:"database_url"`
	Channels        []string       `mapstructure:"channels"`
	Adapters        []string       `mapstructure:"adapters"`
	LogLevel        string         `mapstructure:"log_level"`
	LogFormat       string         `mapstructure:"log_format"`
	ShutdownTimeout time.Duration  `mapstructure:"shutdown_timeout"`
	MetricsAddr     string         `mapstructure:"metrics_addr"`
	Bus             BusConfig      `mapstructure:"bus"`
	Webhook         WebhookConfig  `mapstructure:"webhook"`
	SSE             SSEConfig      `mapstructure:"sse"`
	Detector        DetectorConfig `mapstructure:"detector"`
}

type BusConfig struct {
	BufferSize int `mapstructure:"buffer_size"`
}

type WebhookConfig struct {
	URL         string            `mapstructure:"url"`
	Headers     map[string]string `mapstructure:"headers"`
	SigningKey  string            `mapstructure:"signing_key"`
	MaxRetries  int               `mapstructure:"max_retries"`
	Timeout     time.Duration     `mapstructure:"timeout"`
	BackoffBase time.Duration     `mapstructure:"backoff_base"`
	BackoffCap  time.Duration     `mapstructure:"backoff_cap"`
}

type SSEConfig struct {
	Addr              string        `mapstructure:"addr"`
	CORSOrigins       []string      `mapstructure:"cors_origins"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`
	ReadTimeout       time.Duration `mapstructure:"read_timeout"`
	IdleTimeout       time.Duration `mapstructure:"idle_timeout"`
}

type DetectorConfig struct {
	Type        string        `mapstructure:"type"`
	BackoffBase time.Duration `mapstructure:"backoff_base"`
	BackoffCap  time.Duration `mapstructure:"backoff_cap"`
	Publication string        `mapstructure:"publication"`
}

func Default() Config {
	return Config{
		Channels:        []string{},
		Adapters:        []string{"stdout"},
		LogLevel:        "info",
		LogFormat:       "text",
		ShutdownTimeout: 5 * time.Second,
		Bus: BusConfig{
			BufferSize: 1024,
		},
		Webhook: WebhookConfig{
			MaxRetries:  5,
			Headers:     map[string]string{},
			Timeout:     10 * time.Second,
			BackoffBase: 1 * time.Second,
			BackoffCap:  32 * time.Second,
		},
		SSE: SSEConfig{
			Addr:              ":8080",
			CORSOrigins:       []string{"*"},
			HeartbeatInterval: 15 * time.Second,
			ReadTimeout:       5 * time.Second,
			IdleTimeout:       120 * time.Second,
		},
		Detector: DetectorConfig{
			Type:        "listen_notify",
			BackoffBase: 5 * time.Second,
			BackoffCap:  60 * time.Second,
		},
	}
}
