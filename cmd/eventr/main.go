package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/deeploop-ai/eventr/internal/config"
	"github.com/deeploop-ai/eventr/internal/engine"
	"github.com/deeploop-ai/eventr/internal/observability"
	"github.com/deeploop-ai/eventr/internal/registry"
	"github.com/deeploop-ai/eventr/internal/topology"
	_ "github.com/deeploop-ai/eventr/plugins/all"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "run":
		runCmd(os.Args[2:])
	case "validate":
		validateCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: eventr <run|validate> [flags]\n")
}

func runCmd(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	configPath := fs.String("config", "", "pipeline config file")
	configDir := fs.String("config-dir", "", "directory of pipeline configs")
	format := fs.String("format", "", "config format (yaml|hocon)")
	_ = fs.Parse(args)

	cfgs, err := loadConfigs(*configPath, *configDir, *format)
	if err != nil {
		fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	eng := engine.New(registry.Default)
	for _, cfg := range cfgs {
		ir, err := topology.FromConfig(cfg)
		if err != nil {
			fatal(err)
		}
		for _, w := range ir.DeprecationWarnings {
			fmt.Fprintf(os.Stderr, "warning: %s\n", w)
		}
		if err := eng.Load(ctx, ir); err != nil {
			fatal(err)
		}
		fmt.Printf("loaded pipeline %q (%d stages, %d edges)\n", ir.Name, len(ir.Stages), len(ir.Edges))
	}

	obsCfg := observability.MergeObservability(cfgs)
	if err := eng.StartObservability(ctx, obsCfg); err != nil {
		fatal(err)
	}
	if obsCfg.Metrics.Enabled {
		slog.Info("observability listening",
			"metrics_port", obsCfg.Metrics.Port,
			"metrics_path", obsCfg.Metrics.Path,
			"health_port", obsCfg.Health.Port,
		)
	}

	if err := eng.Start(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("eventr running; press Ctrl+C to stop")
	<-ctx.Done()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	_ = eng.Stop(stopCtx)
	_ = eng.StopObservability(stopCtx)
}

func validateCmd(args []string) {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	configPath := fs.String("config", "", "pipeline config file")
	configDir := fs.String("config-dir", "", "directory of pipeline configs")
	format := fs.String("format", "", "config format (yaml|hocon)")
	_ = fs.Parse(args)

	cfgs, err := loadConfigs(*configPath, *configDir, *format)
	if err != nil {
		fatal(err)
	}
	for _, cfg := range cfgs {
		ir, err := topology.FromConfig(cfg)
		if err != nil {
			fatal(err)
		}
		for _, w := range ir.DeprecationWarnings {
			fmt.Fprintf(os.Stderr, "warning: %s\n", w)
		}
		fmt.Printf("OK: pipeline %q (%d stages, %d edges)\n", ir.Name, len(ir.Stages), len(ir.Edges))
	}
}

func loadConfigs(path, dir, format string) ([]*config.PipelineConfig, error) {
	switch {
	case path != "":
		if format != "" {
			f, err := config.DetectFormat(path, format)
			if err != nil {
				return nil, err
			}
			switch f {
			case config.FormatYAML:
				cfg, err := config.LoadYAML(path)
				return []*config.PipelineConfig{cfg}, err
			case config.FormatHOCON:
				cfg, err := config.LoadHOCON(path)
				return []*config.PipelineConfig{cfg}, err
			}
		}
		cfg, err := config.LoadFile(path)
		return []*config.PipelineConfig{cfg}, err
	case dir != "":
		return config.LoadDir(dir)
	default:
		return nil, fmt.Errorf("either --config or --config-dir is required")
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
