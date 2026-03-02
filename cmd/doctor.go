package cmd

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/florinutz/pgcdc/internal/config"
	"github.com/florinutz/pgcdc/internal/output"
)

type doctorCheck struct {
	Check  string `json:"check"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check system prerequisites and configuration health",
	Long: `Runs a series of diagnostic checks to verify that the local environment,
configuration, database connectivity, and adapter endpoints are correctly set up.`,
	RunE: runDoctor,
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	f := doctorCmd.Flags()
	f.String("db", "", "PostgreSQL connection string (env: PGCDC_DATABASE_URL)")
	output.AddOutputFlag(doctorCmd)
}

func runDoctor(cmd *cobra.Command, _ []string) error {
	var checks []doctorCheck

	// 1. Docker check.
	checks = append(checks, doctorCheckDocker(cmd.Context()))

	// 2. Config check.
	cfg, cfgOK := checkConfig(&checks)

	// 3. Resolve database URL: flag > viper > env.
	dbURL := resolveDBURL(cmd)

	// 4. Database check.
	dbOK := false
	if dbURL != "" {
		c, ok := checkDatabase(cmd.Context(), dbURL)
		checks = append(checks, c)
		dbOK = ok
	} else {
		checks = append(checks, doctorCheck{
			Check:  "database",
			Status: "SKIP",
			Detail: "no connection string (use --db or PGCDC_DATABASE_URL)",
		})
	}

	// 5. Slot check.
	if dbOK {
		checks = append(checks, checkSlot(cmd.Context(), dbURL))
	} else {
		checks = append(checks, doctorCheck{
			Check:  "slot",
			Status: "SKIP",
			Detail: "database not available",
		})
	}

	// 6. WAL check.
	if dbOK {
		checks = append(checks, checkWAL(cmd.Context(), dbURL))
	} else {
		checks = append(checks, doctorCheck{
			Check:  "wal",
			Status: "SKIP",
			Detail: "database not available",
		})
	}

	// 7. Adapter endpoint checks.
	if cfgOK && cfg != nil {
		checks = append(checks, checkAdapterEndpoints(cmd.Context(), cfg)...)
	} else {
		checks = append(checks, doctorCheck{
			Check:  "adapters",
			Status: "SKIP",
			Detail: "no valid config found",
		})
	}

	// Output results.
	printer := output.FromCommand(cmd)
	if printer.IsJSON() {
		return printer.JSON(checks)
	}

	headers := []string{"CHECK", "STATUS", "DETAIL"}
	var rows [][]string
	for _, c := range checks {
		rows = append(rows, []string{c.Check, c.Status, c.Detail})
	}
	return printer.Table(headers, rows)
}

func resolveDBURL(cmd *cobra.Command) string {
	dbURL, _ := cmd.Flags().GetString("db")
	if dbURL != "" {
		return dbURL
	}
	if v := viper.GetString("database_url"); v != "" {
		return v
	}
	return ""
}

func doctorCheckDocker(ctx context.Context) doctorCheck {
	execCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(execCtx, "docker", "version", "--format", "{{.Server.Version}}").Output()
	if err != nil {
		// Check if docker is not found vs. other errors.
		if execErr, ok := err.(*exec.Error); ok && execErr.Err == exec.ErrNotFound {
			return doctorCheck{
				Check:  "docker",
				Status: "SKIP",
				Detail: "docker not found in PATH",
			}
		}
		return doctorCheck{
			Check:  "docker",
			Status: "SKIP",
			Detail: fmt.Sprintf("docker not available: %s", err),
		}
	}

	version := strings.TrimSpace(string(out))
	return doctorCheck{
		Check:  "docker",
		Status: "OK",
		Detail: fmt.Sprintf("server version %s", version),
	}
}

func checkConfig(checks *[]doctorCheck) (*config.Config, bool) {
	cfg := config.Default()
	if err := viper.Unmarshal(&cfg); err != nil {
		*checks = append(*checks, doctorCheck{
			Check:  "config",
			Status: "FAIL",
			Detail: fmt.Sprintf("unmarshal: %s", err),
		})
		return nil, false
	}

	if err := cfg.Validate(); err != nil {
		*checks = append(*checks, doctorCheck{
			Check:  "config",
			Status: "FAIL",
			Detail: err.Error(),
		})
		return &cfg, false
	}

	if viper.ConfigFileUsed() != "" {
		*checks = append(*checks, doctorCheck{
			Check:  "config",
			Status: "OK",
			Detail: fmt.Sprintf("loaded %s", viper.ConfigFileUsed()),
		})
	} else {
		*checks = append(*checks, doctorCheck{
			Check:  "config",
			Status: "SKIP",
			Detail: "no config file found (using defaults)",
		})
	}
	return &cfg, true
}

func checkDatabase(ctx context.Context, dbURL string) (doctorCheck, bool) {
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	start := time.Now()
	conn, err := pgx.Connect(connCtx, dbURL)
	if err != nil {
		return doctorCheck{
			Check:  "database",
			Status: "FAIL",
			Detail: fmt.Sprintf("connect: %s", err),
		}, false
	}
	defer func() { _ = conn.Close(ctx) }()

	var version string
	if err := conn.QueryRow(connCtx, "SELECT version()").Scan(&version); err != nil {
		return doctorCheck{
			Check:  "database",
			Status: "FAIL",
			Detail: fmt.Sprintf("query: %s", err),
		}, false
	}

	latency := time.Since(start).Truncate(time.Millisecond)
	// Truncate the version string to something readable.
	if idx := strings.Index(version, " on "); idx > 0 {
		version = version[:idx]
	}

	return doctorCheck{
		Check:  "database",
		Status: "OK",
		Detail: fmt.Sprintf("%s (latency: %s)", version, latency),
	}, true
}

func checkSlot(ctx context.Context, dbURL string) doctorCheck {
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, dbURL)
	if err != nil {
		return doctorCheck{
			Check:  "slot",
			Status: "FAIL",
			Detail: fmt.Sprintf("connect: %s", err),
		}
	}
	defer func() { _ = conn.Close(ctx) }()

	rows, err := conn.Query(connCtx,
		"SELECT slot_name, active FROM pg_replication_slots WHERE slot_name LIKE 'pgcdc_%'")
	if err != nil {
		return doctorCheck{
			Check:  "slot",
			Status: "FAIL",
			Detail: fmt.Sprintf("query: %s", err),
		}
	}
	defer rows.Close()

	var total, inactive int
	var slotNames []string
	for rows.Next() {
		var name string
		var active bool
		if err := rows.Scan(&name, &active); err != nil {
			continue
		}
		total++
		slotNames = append(slotNames, name)
		if !active {
			inactive++
		}
	}
	if err := rows.Err(); err != nil {
		return doctorCheck{
			Check:  "slot",
			Status: "FAIL",
			Detail: fmt.Sprintf("scan: %s", err),
		}
	}

	if total == 0 {
		return doctorCheck{
			Check:  "slot",
			Status: "OK",
			Detail: "no pgcdc_* replication slots found",
		}
	}

	detail := fmt.Sprintf("%d slot(s): %s", total, strings.Join(slotNames, ", "))
	if inactive > 0 {
		return doctorCheck{
			Check:  "slot",
			Status: "WARN",
			Detail: fmt.Sprintf("%s (%d inactive — may cause WAL retention)", detail, inactive),
		}
	}

	return doctorCheck{
		Check:  "slot",
		Status: "OK",
		Detail: detail,
	}
}

func checkWAL(ctx context.Context, dbURL string) doctorCheck {
	connCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := pgx.Connect(connCtx, dbURL)
	if err != nil {
		return doctorCheck{
			Check:  "wal",
			Status: "FAIL",
			Detail: fmt.Sprintf("connect: %s", err),
		}
	}
	defer func() { _ = conn.Close(ctx) }()

	var walLevel string
	if err := conn.QueryRow(connCtx, "SHOW wal_level").Scan(&walLevel); err != nil {
		return doctorCheck{
			Check:  "wal",
			Status: "FAIL",
			Detail: fmt.Sprintf("query wal_level: %s", err),
		}
	}

	var maxSlots string
	if err := conn.QueryRow(connCtx, "SHOW max_replication_slots").Scan(&maxSlots); err != nil {
		return doctorCheck{
			Check:  "wal",
			Status: "FAIL",
			Detail: fmt.Sprintf("query max_replication_slots: %s", err),
		}
	}

	if walLevel != "logical" {
		return doctorCheck{
			Check:  "wal",
			Status: "WARN",
			Detail: fmt.Sprintf("wal_level=%s (need 'logical' for WAL detector), max_replication_slots=%s", walLevel, maxSlots),
		}
	}

	return doctorCheck{
		Check:  "wal",
		Status: "OK",
		Detail: fmt.Sprintf("wal_level=logical, max_replication_slots=%s", maxSlots),
	}
}

func checkAdapterEndpoints(ctx context.Context, cfg *config.Config) []doctorCheck {
	type endpoint struct {
		adapter  string
		hostname string
	}

	var endpoints []endpoint

	for _, a := range cfg.Adapters {
		switch a {
		case "webhook":
			if h := extractHost(cfg.Webhook.URL); h != "" {
				endpoints = append(endpoints, endpoint{"webhook", h})
			}
		case "kafka":
			for _, broker := range cfg.Kafka.Brokers {
				if h := extractHostPort(broker); h != "" {
					endpoints = append(endpoints, endpoint{"kafka", h})
				}
			}
		case "nats":
			if h := extractHost(cfg.Nats.URL); h != "" {
				endpoints = append(endpoints, endpoint{"nats", h})
			}
		case "redis":
			if h := extractHost(cfg.Redis.URL); h != "" {
				endpoints = append(endpoints, endpoint{"redis", h})
			}
		case "search":
			if h := extractHost(cfg.Search.URL); h != "" {
				endpoints = append(endpoints, endpoint{"search", h})
			}
		case "grpc":
			if h := extractHostPort(cfg.GRPC.Addr); h != "" {
				endpoints = append(endpoints, endpoint{"grpc", h})
			}
		}
	}

	if len(endpoints) == 0 {
		return []doctorCheck{{
			Check:  "adapters",
			Status: "SKIP",
			Detail: "no adapter endpoints to check",
		}}
	}

	var checks []doctorCheck
	for _, ep := range endpoints {
		c := lookupHost(ctx, ep.adapter, ep.hostname)
		checks = append(checks, c)
	}
	return checks
}

func lookupHost(ctx context.Context, adapter, hostname string) doctorCheck {
	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	checkName := fmt.Sprintf("dns/%s", adapter)

	// Skip DNS for localhost and IP addresses.
	if hostname == "localhost" || net.ParseIP(hostname) != nil {
		return doctorCheck{
			Check:  checkName,
			Status: "OK",
			Detail: fmt.Sprintf("%s (local)", hostname),
		}
	}

	addrs, err := net.DefaultResolver.LookupHost(lookupCtx, hostname)
	if err != nil {
		return doctorCheck{
			Check:  checkName,
			Status: "FAIL",
			Detail: fmt.Sprintf("lookup %s: %s", hostname, err),
		}
	}

	return doctorCheck{
		Check:  checkName,
		Status: "OK",
		Detail: fmt.Sprintf("%s -> %s", hostname, strings.Join(addrs, ", ")),
	}
}

// extractHost parses a URL and returns the hostname (without port).
func extractHost(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}

// extractHostPort parses a host:port or :port string and returns the hostname.
// Returns empty for :port-only addresses (listen addresses like ":9090").
func extractHostPort(addr string) string {
	if addr == "" {
		return ""
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Maybe it's just a hostname without port.
		return addr
	}
	if host == "" {
		return "" // :port only — listen address, not a remote endpoint
	}
	return host
}
