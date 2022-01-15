package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"fassst/pkg/fs"
)

const Version = "v0.1.0"

func NewVersionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "version",
		Short:   "Prints version information",
		Example: "fassst version",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("cmd version:", Version)
			fmt.Println("pkg version:", fs.Version)
		},
	}

	return command
}
