package main

import (
	"fmt"
	"os"

	"github.com/FarelRA/storhub/internal/cli"
)

func main() {
	app := cli.New()
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if cli.IsUsageError(err) {
			// 2: the command line itself was wrong (flags, arguments).
			os.Exit(2)
		}
		// 1: the command was well-formed but the operation failed.
		os.Exit(1)
	}
}
