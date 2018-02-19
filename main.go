package main

import (
	"os"

	"github.com/Ullaakut/hackerbeat/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
