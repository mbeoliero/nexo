package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/mbeoliero/nexo/server"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "nexo:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: nexo <serve|migrate> [-config path]")
	}
	fs := flag.NewFlagSet("nexo "+args[0], flag.ContinueOnError)
	cfgPath := fs.String("config", os.Getenv("NEXO_CONFIG"), "path to yaml config (env NEXO_CONFIG)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	ctx := context.Background()
	switch args[0] {
	case "serve":
		cfg, err := server.LoadConfig(*cfgPath)
		if err != nil {
			return err
		}
		return server.ListenAndServe(ctx, cfg)
	case "migrate":
		db, err := server.LoadDbConfig(*cfgPath)
		if err != nil {
			return err
		}
		return server.Migrate(ctx, db)
	default:
		return fmt.Errorf("unknown command %q, want serve|migrate", args[0])
	}
}
