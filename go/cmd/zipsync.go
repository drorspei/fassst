package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"fassst/pkg/fs"
)

func NewZipSyncCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:     "zipsync",
		Aliases: []string{"zsync", "zs"},
		Short:   "copy missing or updated files and archive them",
		Example: "fassst zsync path/to/source path/to/target",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("cmd version:", Version)
			fmt.Println("pkg version:", fs.Version)
			fmt.Println("command not implemented")
		},
	}

	return command
}
