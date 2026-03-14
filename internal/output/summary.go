package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/florinutz/pgcdc/internal/config"
)

const (
	summaryWidth = 50 // content area width (between the | chars)

	ansiCyan  = "\033[36m"
	ansiReset = "\033[0m"
)

// PrintSummary writes a compact startup banner to w summarizing the pipeline
// configuration. When useColor is true, box borders are rendered in cyan ANSI.
func PrintSummary(w io.Writer, cfg *config.Config, useColor bool) {
	if cfg == nil {
		return
	}

	border := func(s string) string {
		if useColor {
			return ansiCyan + s + ansiReset
		}
		return s
	}

	// Collect lines as label:value pairs.
	type line struct {
		label string
		value string
	}
	var lines []line

	// Detector.
	det := cfg.Detector.Type
	if det == "" {
		det = "listen_notify"
	}
	if cfg.Detector.CooperativeCheckpoint {
		det += " (cooperative-ckpt)"
	}
	lines = append(lines, line{"Detector", det})

	// Adapters.
	if len(cfg.Adapters) > 0 {
		lines = append(lines, line{"Adapters", strings.Join(cfg.Adapters, ", ")})
	}

	// Channels.
	if len(cfg.Channels) > 0 {
		lines = append(lines, line{"Channels", strings.Join(cfg.Channels, ", ")})
	}

	// Bus.
	busMode := cfg.Bus.Mode
	if busMode == "" {
		busMode = "fast"
	}
	bufSize := cfg.Bus.BufferSize
	if bufSize == 0 {
		bufSize = 1024
	}
	lines = append(lines, line{"Bus", fmt.Sprintf("%s (buffer: %d)", busMode, bufSize)})

	// Features.
	var features []string
	if cfg.Backpressure.Enabled {
		features = append(features, "backpressure")
	}
	if cfg.Inspector.BufferSize > 0 {
		features = append(features, "inspect")
	}
	if cfg.Detector.ToastCache {
		features = append(features, "toast-cache")
	}
	if cfg.Schema.Enabled {
		features = append(features, "schema-store")
	}
	if cfg.Detector.SchemaEvents {
		features = append(features, "schema-events")
	}
	if cfg.Detector.TxMetadata {
		features = append(features, "tx-metadata")
	}
	if cfg.Detector.TxMarkers {
		features = append(features, "tx-markers")
	}
	if cfg.Detector.SnapshotFirst {
		features = append(features, "snapshot-first")
	}
	if len(features) > 0 {
		lines = append(lines, line{"Features", strings.Join(features, ", ")})
	}

	// Endpoints.
	httpAdapters := map[string]bool{
		"sse": true, "ws": true, "graphql": true, "webhookgw": true,
		"duckdb": true, "arrow": true, "kafkaserver": true,
	}
	var httpFeatures []string
	for _, a := range cfg.Adapters {
		if httpAdapters[a] {
			httpFeatures = append(httpFeatures, a)
		}
	}

	var endpointLines []line
	if len(httpFeatures) > 0 {
		addr := cfg.SSE.Addr
		if addr == "" {
			addr = ":8080"
		}
		endpointLines = append(endpointLines, line{
			"Endpoints",
			fmt.Sprintf("%s (%s)", addr, strings.Join(httpFeatures, ",")),
		})
	}
	if cfg.MetricsAddr != "" {
		label := "Endpoints"
		if len(endpointLines) > 0 {
			label = "" // continuation line
		}
		endpointLines = append(endpointLines, line{
			label,
			fmt.Sprintf("%s (metrics)", cfg.MetricsAddr),
		})
	}
	lines = append(lines, endpointLines...)

	// Render the box.
	// Top border.
	title := " pgcdc "
	topPadding := summaryWidth - len(title) - 2 // -2 for "+ " prefix area
	if topPadding < 0 {
		topPadding = 0
	}
	topLine := "+--" + title + strings.Repeat("-", topPadding) + "+"
	_, _ = fmt.Fprintln(w, border(topLine))

	// Content lines.
	for _, l := range lines {
		var content string
		if l.label != "" {
			content = fmt.Sprintf("%-12s%s", l.label+":", l.value)
		} else {
			// Continuation line (e.g. second endpoint).
			content = fmt.Sprintf("%-12s%s", "", l.value)
		}

		// Truncate if needed, then pad to fill width.
		// Available content width = summaryWidth - 4 (for "| " prefix and " |" suffix).
		contentWidth := summaryWidth - 4
		if len(content) > contentWidth {
			content = content[:contentWidth]
		}
		padded := content + strings.Repeat(" ", contentWidth-len(content))

		_, _ = fmt.Fprintf(w, "%s %s %s\n",
			border("|"), padded, border("|"))
	}

	// Bottom border.
	bottomLine := "+" + strings.Repeat("-", summaryWidth-2) + "+"
	_, _ = fmt.Fprintln(w, border(bottomLine))
}
