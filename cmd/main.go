package main

import (
	"fmt"
	"os"

	cli "github.com/urfave/cli/v2"

	zkevmbridgeservice "github.com/0xPolygonHermez/zkevm-bridge-service"
)

const (
	flagCfg     = "cfg"
	flagNetwork = "network"
)

const (
	// App name
	appName = "zkevm-bridge"
)

func main() {
	app := cli.NewApp()
	app.Name = appName
	app.Version = zkevmbridgeservice.Version
	flags := []cli.Flag{
		&cli.StringFlag{
			Name:     flagCfg,
			Aliases:  []string{"c"},
			Usage:    "Configuration `FILE`",
			Required: false,
		},
		&cli.StringFlag{
			Name:     flagNetwork,
			Aliases:  []string{"n"},
			Usage:    "Network: mainnet, testnet, internaltestnet, local. By default it uses mainnet",
			Required: false,
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:    "version",
			Aliases: []string{},
			Usage:   "Application version and build",
			Action:  versionCmd,
		},
		{
			Name:    "run",
			Aliases: []string{},
			Usage:   "Run the zkevm bridge",
			Action:  start,
			Flags:   flags,
		},
		{
			Name:    "runAll",
			Aliases: []string{},
			Usage:   "Run the xlayer bridge as a single binary",
			Action: func(ctx *cli.Context) error {
				return run(ctx, "all")
			},
			Flags: flags,
		},
		{
			Name:    "runAPI",
			Aliases: []string{},
			Usage:   "Run the xlayer bridge API server",
			Action: func(ctx *cli.Context) error {
				return run(ctx, api)
			},
			Flags: flags,
		},
		{
			Name:    "runPushTask",
			Aliases: []string{},
			Usage:   "Run the xlayer bridge push tasks (monitor the block/batch number and push change event to FE)",
			Action: func(ctx *cli.Context) error {
				return run(ctx, push)
			},
			Flags: flags,
		},
		{
			Name:    "runTask",
			Aliases: []string{},
			Usage:   "Run the xlayer bridge tasks, including synchronizer, claimtxman, kafka consumer",
			Action: func(ctx *cli.Context) error {
				return run(ctx, task)
			},
			Flags: flags,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		os.Exit(1)
	}
}
