package cmd

import (
	"fmt"

	"github.com/florinutz/pgcdc/checkpoint"
	"github.com/florinutz/pgcdc/detector/walreplication"
	"github.com/florinutz/pgcdc/registry"
	"github.com/florinutz/pgcdc/snapshot"
)

func init() {
	registry.RegisterDetector(registry.DetectorEntry{
		Name:        "wal",
		Description: "PostgreSQL WAL logical replication",
		ViperKeys: [][2]string{
			{"publication", "detector.publication"},
			{"persistent-slot", "detector.persistent_slot"},
			{"slot-name", "detector.slot_name"},
			{"tx-metadata", "detector.tx_metadata"},
			{"include-schema", "detector.include_schema"},
		},
		Spec: []registry.ParamSpec{
			{
				Name:        "publication",
				Type:        "string",
				Default:     "pgcdc_pub",
				Required:    true,
				Description: "PostgreSQL publication name to subscribe to",
			},
			{
				Name:        "persistent-slot",
				Type:        "bool",
				Default:     false,
				Description: "Create a named, non-temporary replication slot with LSN checkpointing",
			},
			{
				Name:        "slot-name",
				Type:        "string",
				Description: "Replication slot name (default: pgcdc_<publication>)",
			},
			{
				Name:        "tx-metadata",
				Type:        "bool",
				Default:     false,
				Description: "Enrich events with transaction.xid, transaction.commit_time, transaction.seq",
			},
			{
				Name:        "include-schema",
				Type:        "bool",
				Default:     false,
				Description: "Add column type metadata (name, type_oid, type_name) to WAL events",
			},
		},
		Create: func(ctx registry.DetectorContext) (registry.DetectorResult, error) {
			cfg := ctx.Cfg
			det := walreplication.New(
				cfg.DatabaseURL, cfg.Detector.Publication,
				cfg.Detector.BackoffBase, cfg.Detector.BackoffCap,
				cfg.Detector.TxMetadata, cfg.Detector.TxMarkers,
				ctx.Logger,
			)

			var cpStore checkpoint.Store
			cleanup := func() {}

			if cfg.Detector.SnapshotFirst {
				det.SetSnapshotFirst(cfg.Snapshot.Table, cfg.Snapshot.Where, cfg.Snapshot.BatchSize)
			}

			if cfg.Detector.PersistentSlot {
				slotName := cfg.Detector.SlotName
				if slotName == "" {
					slotName = "pgcdc_" + cfg.Detector.Publication
				}
				det.SetPersistentSlot(slotName)

				// Checkpoint store: prefer wasm plugin, fall back to PG.
				cpPluginPath := ""
				cpPluginCfg := map[string]any{}
				if cfg.Plugins.Checkpoint != nil {
					cpPluginPath = cfg.Plugins.Checkpoint.Path
					if cfg.Plugins.Checkpoint.Config != nil {
						cpPluginCfg = cfg.Plugins.Checkpoint.Config
					}
				}

				if cpPluginPath != "" {
					store, err := makePluginCheckpoint(ctx.Ctx, ctx.WasmRT, cpPluginPath, cpPluginCfg, ctx.Logger)
					if err != nil {
						return registry.DetectorResult{}, fmt.Errorf("create wasm checkpoint store: %w", err)
					}
					cpStore = store
				} else {
					cpDB := cfg.Detector.CheckpointDB
					if cpDB == "" {
						cpDB = cfg.DatabaseURL
					}
					store, err := checkpoint.NewPGStore(ctx.Ctx, cpDB, ctx.Logger)
					if err != nil {
						return registry.DetectorResult{}, fmt.Errorf("create checkpoint store: %w", err)
					}
					cpStore = store
				}
				det.SetCheckpointStore(cpStore)
			}

			if cfg.Detector.IncludeSchema {
				det.SetIncludeSchema(true)
			}
			if cfg.Detector.SchemaEvents {
				det.SetSchemaEvents(true)
			}
			if cfg.Detector.HeartbeatInterval > 0 {
				det.SetHeartbeat(cfg.Detector.HeartbeatInterval, cfg.Detector.HeartbeatTable, cfg.DatabaseURL)
			}
			if cfg.Detector.SlotLagWarn > 0 {
				det.SetSlotLagWarn(cfg.Detector.SlotLagWarn)
			}
			if cfg.Detector.ToastCache {
				det.SetToastCache(cfg.Detector.ToastCacheMaxEntries)
			}
			if cfg.IncrementalSnapshot.Enabled {
				det.SetIncrementalSnapshot(
					cfg.IncrementalSnapshot.SignalTable,
					cfg.IncrementalSnapshot.ChunkSize,
					cfg.IncrementalSnapshot.ChunkDelay,
				)
				progDB := cfg.IncrementalSnapshot.ProgressDB
				if progDB == "" {
					progDB = cfg.DatabaseURL
				}
				progStore, err := snapshot.NewPGProgressStore(ctx.Ctx, progDB, ctx.Logger)
				if err != nil {
					return registry.DetectorResult{}, fmt.Errorf("create snapshot progress store: %w", err)
				}
				det.SetProgressStore(progStore)
				cleanup = func() { _ = progStore.Close() }
			}

			return registry.DetectorResult{
				Detector:        det,
				CheckpointStore: cpStore,
				Cleanup:         cleanup,
			}, nil
		},
	})
}
