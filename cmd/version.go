package cmd

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/florinutz/pgcdc/internal/output"
	"github.com/spf13/cobra"
)

// Version, Commit, and BuildDate are set at build time via ldflags:
//
//	go build -ldflags "-X github.com/florinutz/pgcdc/cmd.Version=v1.0.0 -X github.com/florinutz/pgcdc/cmd.Commit=abc1234 -X github.com/florinutz/pgcdc/cmd.BuildDate=2026-01-01T00:00:00Z"
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// slimBuildTags lists build tags that indicate a slim binary.
var slimBuildTags = []string{
	"no_kafka", "no_grpc", "no_iceberg", "no_nats", "no_redis",
	"no_plugins", "no_kafkaserver", "no_views", "no_s3",
	"no_graphql", "no_arrow", "no_duckdb", "no_sqlite",
}

type versionInfo struct {
	Version   string   `json:"version"`
	Commit    string   `json:"commit"`
	BuildDate string   `json:"build_date"`
	Go        string   `json:"go"`
	OS        string   `json:"os"`
	Arch      string   `json:"arch"`
	BuildTags []string `json:"build_tags,omitempty"`
	Build     string   `json:"build"`
}

func detectBuildTags() []string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}

	for _, s := range info.Settings {
		if s.Key == "-tags" && s.Value != "" {
			return strings.Split(s.Value, ",")
		}
	}
	return nil
}

func buildLabel(tags []string) string {
	if len(tags) == 0 {
		return "full"
	}
	tagSet := make(map[string]bool, len(tags))
	for _, t := range tags {
		tagSet[t] = true
	}
	for _, st := range slimBuildTags {
		if tagSet[st] {
			return "slim"
		}
	}
	return "full"
}

func getVersionInfo() versionInfo {
	tags := detectBuildTags()
	return versionInfo{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
		Go:        runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		BuildTags: tags,
		Build:     buildLabel(tags),
	}
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the pgcdc version",
	RunE: func(cmd *cobra.Command, args []string) error {
		printer := output.FromCommand(cmd)
		info := getVersionInfo()

		if printer.IsJSON() {
			return printer.JSON(info)
		}

		w := printer.Writer()
		_, _ = fmt.Fprintf(w, "pgcdc %s\n", info.Version)
		_, _ = fmt.Fprintf(w, "  commit:     %s\n", info.Commit)
		_, _ = fmt.Fprintf(w, "  built:      %s\n", info.BuildDate)
		_, _ = fmt.Fprintf(w, "  go:         %s\n", info.Go)
		_, _ = fmt.Fprintf(w, "  os/arch:    %s/%s\n", info.OS, info.Arch)
		_, _ = fmt.Fprintf(w, "  build:      %s\n", info.Build)
		return nil
	},
}

func init() {
	output.AddOutputFlag(versionCmd)
}
