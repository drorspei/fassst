package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"fassst/pkg/fassst"
	"fassst/pkg/fs"
)

const Version = "v0.4.1"

func NewVersionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:     "version",
		Short:   "Prints version information",
		Example: "fassst version",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("cmd version:", Version)
			fmt.Println("pkg/fs version:", fs.Version)
			fmt.Println("pkg/fassst version:", fassst.Version)
		},
	}

	return command
}
