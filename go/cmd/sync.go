package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"fassst/pkg/fs"
)

func NewSyncCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:     "sync",
		Short:   "copy missing or updated files",
		Example: "fassst sync path/to/source path/to/target",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("cmd version:", Version)
			fmt.Println("pkg version:", fs.Version)
			fmt.Println("command not implemented")
		},
	}

	return command
}
