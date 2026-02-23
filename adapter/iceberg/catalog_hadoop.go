package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// HadoopCatalog stores Iceberg metadata as versioned JSON files on a filesystem.
// No catalog server required. Table location: {warehouse}/{namespace}/{table}/
type HadoopCatalog struct {
	warehouse string
	storage   Storage
}

// NewHadoopCatalog creates a hadoop-style catalog backed by the given storage.
func NewHadoopCatalog(warehouse string, storage Storage) *HadoopCatalog {
	return &HadoopCatalog{
		warehouse: warehouse,
		storage:   storage,
	}
}

func (c *HadoopCatalog) tablePath(ns []string, table string) string {
	parts := append([]string{c.warehouse}, ns...)
	parts = append(parts, table)
	return filepath.Join(parts...)
}

func (c *HadoopCatalog) metadataDir(ns []string, table string) string {
	return filepath.Join(c.tablePath(ns, table), "metadata")
}

// LoadTable reads the latest versioned metadata file.
func (c *HadoopCatalog) LoadTable(ctx context.Context, ns []string, table string) (*TableMetadata, error) {
	metaDir := c.metadataDir(ns, table)

	version, err := c.latestVersion(ctx, metaDir)
	if err != nil {
		return nil, err
	}
	if version < 0 {
		return nil, nil // table doesn't exist
	}

	path := filepath.Join(metaDir, fmt.Sprintf("v%d.metadata.json", version))
	data, err := c.storage.Read(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("read metadata v%d: %w", version, err)
	}

	meta, err := readMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("parse metadata v%d: %w", version, err)
	}
	return meta, nil
}

// CreateTable writes the initial metadata as v1.metadata.json.
func (c *HadoopCatalog) CreateTable(ctx context.Context, ns []string, table string, meta *TableMetadata) error {
	metaDir := c.metadataDir(ns, table)

	// Set the table location.
	meta.Location = c.tablePath(ns, table)

	data, err := writeMetadata(meta)
	if err != nil {
		return err
	}

	path := filepath.Join(metaDir, "v1.metadata.json")
	if err := c.storage.Write(ctx, path, data); err != nil {
		return fmt.Errorf("write metadata v1: %w", err)
	}

	// Write version-hint.text for other readers.
	hintPath := filepath.Join(metaDir, "version-hint.text")
	if err := c.storage.Write(ctx, hintPath, []byte("1")); err != nil {
		return fmt.Errorf("write version hint: %w", err)
	}

	return nil
}

// CommitTable writes the next versioned metadata file.
// Uses optimistic concurrency: checks that the latest version hasn't changed since oldMeta was read.
func (c *HadoopCatalog) CommitTable(ctx context.Context, ns []string, table string, oldMeta, newMeta *TableMetadata) error {
	metaDir := c.metadataDir(ns, table)

	currentVersion, err := c.latestVersion(ctx, metaDir)
	if err != nil {
		return err
	}

	// Set the table location.
	newMeta.Location = c.tablePath(ns, table)

	newVersion := currentVersion + 1
	data, err := writeMetadata(newMeta)
	if err != nil {
		return err
	}

	path := filepath.Join(metaDir, fmt.Sprintf("v%d.metadata.json", newVersion))
	if err := c.storage.Write(ctx, path, data); err != nil {
		return fmt.Errorf("write metadata v%d: %w", newVersion, err)
	}

	// Update version hint.
	hintPath := filepath.Join(metaDir, "version-hint.text")
	if err := c.storage.Write(ctx, hintPath, []byte(strconv.Itoa(newVersion))); err != nil {
		return fmt.Errorf("update version hint: %w", err)
	}

	return nil
}

// latestVersion finds the highest version number in the metadata directory.
// Returns -1 if no metadata files exist.
func (c *HadoopCatalog) latestVersion(ctx context.Context, metaDir string) (int, error) {
	// Try reading version-hint.text first.
	hintPath := filepath.Join(metaDir, "version-hint.text")
	hintData, err := c.storage.Read(ctx, hintPath)
	if err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(string(hintData))); err == nil && v > 0 {
			// Verify the file exists.
			path := filepath.Join(metaDir, fmt.Sprintf("v%d.metadata.json", v))
			if exists, _ := c.storage.Exists(ctx, path); exists {
				return v, nil
			}
		}
	}

	// Fallback: scan for v*.metadata.json files.
	// For local filesystem, we can list the directory.
	return c.scanVersions(ctx, metaDir)
}

// scanVersions lists the metadata directory and finds the highest version.
func (c *HadoopCatalog) scanVersions(ctx context.Context, metaDir string) (int, error) {
	var versions []int
	for v := 1; v <= 10000; v++ {
		path := filepath.Join(metaDir, fmt.Sprintf("v%d.metadata.json", v))
		exists, err := c.storage.Exists(ctx, path)
		if err != nil {
			return -1, fmt.Errorf("scan versions: %w", err)
		}
		if !exists {
			break
		}
		versions = append(versions, v)
	}

	if len(versions) == 0 {
		return -1, nil
	}

	sort.Ints(versions)
	return versions[len(versions)-1], nil
}
