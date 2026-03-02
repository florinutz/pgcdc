package cmd

import (
	"runtime"
	"testing"
)

func TestBuildLabel_Nil(t *testing.T) {
	t.Parallel()
	if got := buildLabel(nil); got != "full" {
		t.Errorf("buildLabel(nil) = %q, want %q", got, "full")
	}
}

func TestBuildLabel_Empty(t *testing.T) {
	t.Parallel()
	if got := buildLabel([]string{}); got != "full" {
		t.Errorf("buildLabel([]) = %q, want %q", got, "full")
	}
}

func TestBuildLabel_SlimTag(t *testing.T) {
	t.Parallel()
	if got := buildLabel([]string{"no_kafka"}); got != "slim" {
		t.Errorf("buildLabel([no_kafka]) = %q, want %q", got, "slim")
	}
}

func TestBuildLabel_MultipleSlimTags(t *testing.T) {
	t.Parallel()
	if got := buildLabel([]string{"no_kafka", "no_grpc", "no_duckdb"}); got != "slim" {
		t.Errorf("buildLabel([no_kafka,no_grpc,no_duckdb]) = %q, want %q", got, "slim")
	}
}

func TestBuildLabel_CustomTag(t *testing.T) {
	t.Parallel()
	if got := buildLabel([]string{"custom"}); got != "full" {
		t.Errorf("buildLabel([custom]) = %q, want %q", got, "full")
	}
}

func TestBuildLabel_MixedTags(t *testing.T) {
	t.Parallel()
	if got := buildLabel([]string{"custom", "no_redis"}); got != "slim" {
		t.Errorf("buildLabel([custom,no_redis]) = %q, want %q", got, "slim")
	}
}

func TestGetVersionInfo(t *testing.T) {
	t.Parallel()
	info := getVersionInfo()

	if info.Go == "" {
		t.Error("Go version should not be empty")
	}
	if info.OS == "" {
		t.Error("OS should not be empty")
	}
	if info.Arch == "" {
		t.Error("Arch should not be empty")
	}
	if info.OS != runtime.GOOS {
		t.Errorf("OS = %q, want %q", info.OS, runtime.GOOS)
	}
	if info.Arch != runtime.GOARCH {
		t.Errorf("Arch = %q, want %q", info.Arch, runtime.GOARCH)
	}
	if info.Version != Version {
		t.Errorf("Version = %q, want %q", info.Version, Version)
	}
}
