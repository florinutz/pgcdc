package cmd

import "testing"

func TestExtractHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"https://example.com:8080/path", "example.com"},
		{"http://localhost:5432/db", "localhost"},
		{"nats://broker.internal:4222", "broker.internal"},
		{"redis://127.0.0.1:6379", "127.0.0.1"},
		{"", ""},
		{"://bad", ""},
	}

	for _, tt := range tests {
		if got := extractHost(tt.input); got != tt.want {
			t.Errorf("extractHost(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExtractHostPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"localhost:9092", "localhost"},
		{"broker.example.com:9092", "broker.example.com"},
		{":9090", ""},
		{"", ""},
		{"just-a-host", "just-a-host"}, // no port, returned as-is
	}

	for _, tt := range tests {
		if got := extractHostPort(tt.input); got != tt.want {
			t.Errorf("extractHostPort(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
