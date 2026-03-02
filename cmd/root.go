package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/florinutz/pgcdc/internal/logcolor"
	"github.com/florinutz/pgcdc/tracing"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "pgcdc",
	Short: "Stream database changes to webhooks, SSE, stdout, and more",
	Long: `pgcdc captures change events from PostgreSQL (LISTEN/NOTIFY, WAL, outbox),
MySQL (binlog), or MongoDB (Change Streams) and fans them out to one or more
adapters: stdout, webhook, SSE, WebSocket, gRPC, file, NATS, Kafka, and others.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return setupLogger()
	},
	SilenceUsage: true,
}

// Execute is called by main.go and is the entry point for the CLI.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default: ./pgcdc.yaml)")
	rootCmd.PersistentFlags().String("log-level", "info", "log level: debug, info, warn, error")
	rootCmd.PersistentFlags().String("log-format", "auto", "log format: text, json, color, auto")

	mustBindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	mustBindPFlag("log_format", rootCmd.PersistentFlags().Lookup("log-format"))

	rootCmd.AddCommand(listenCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(versionCmd)
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("pgcdc")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
	}

	viper.SetEnvPrefix("PGCDC")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			// Only warn if a config file was explicitly specified but could not be read.
			if cfgFile != "" {
				fmt.Fprintf(os.Stderr, "Warning: could not read config file: %v\n", err)
			}
		}
	}
}

func setupLogger() error {
	level := viper.GetString("log_level")
	format := viper.GetString("log_format")

	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		return fmt.Errorf("unknown log level: %q (expected debug, info, warn, error)", level)
	}

	opts := &slog.HandlerOptions{Level: slogLevel}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(os.Stderr, opts)
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "color":
		handler = logcolor.NewHandler(os.Stderr, opts)
	case "auto", "":
		if logcolor.ShouldColor(os.Stderr.Fd(), format) {
			handler = logcolor.NewHandler(os.Stderr, opts)
		} else {
			handler = slog.NewTextHandler(os.Stderr, opts)
		}
	default:
		return fmt.Errorf("unknown log format: %q (expected text, json, color, auto)", format)
	}

	slog.SetDefault(slog.New(tracing.NewTracingHandler(handler)))
	return nil
}

func mustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(fmt.Sprintf("viper.BindPFlag(%q): %v", key, err))
	}
}

// bindFlagsToViper binds a list of flag→viper-key pairs. Flags that don't exist
// (e.g., build-tagged out) are silently skipped.
func bindFlagsToViper(f *pflag.FlagSet, bindings [][2]string) error {
	for _, b := range bindings {
		flag := f.Lookup(b[0])
		if flag == nil {
			continue
		}
		if err := viper.BindPFlag(b[1], flag); err != nil {
			return fmt.Errorf("bind flag %q to %q: %w", b[0], b[1], err)
		}
	}
	return nil
}
