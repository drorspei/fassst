package main

import (
	"os"

	"fassst/cmd"
)

func main() {
	command := cmd.NewCommand()
	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
