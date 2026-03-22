package main

import (
	"fmt"
	"os"

	"github.com/depfloy/dpm/internal/daemon"
	"github.com/depfloy/dpm/pkg/config"
)

func main() {
	configPath := "/etc/dpm/config.yaml"

	// Parse flags
	for i, arg := range os.Args[1:] {
		switch {
		case arg == "--config" && i+1 < len(os.Args)-1:
			configPath = os.Args[i+2]
		case len(arg) > 9 && arg[:9] == "--config=":
			configPath = arg[9:]
		case arg == "--version":
			fmt.Printf("dpmd %s\n", daemon.Version)
			os.Exit(0)
		case arg == "--help":
			fmt.Println("Usage: dpmd [options]")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --config=PATH    Config file path (default: /etc/dpm/config.yaml)")
			fmt.Println("  --version        Show version")
			fmt.Println("  --help           Show help")
			os.Exit(0)
		}
	}

	// Load config
	cfg, err := config.LoadDaemonConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		fmt.Fprintf(os.Stderr, "Using defaults\n")
		cfg = config.DefaultDaemonConfig()
	}

	// Create and run daemon
	d, err := daemon.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create daemon: %v\n", err)
		os.Exit(1)
	}

	if err := d.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Daemon error: %v\n", err)
		os.Exit(1)
	}
}
