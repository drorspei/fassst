package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"fassst/pkg/fs"
)

func NewZipCopyCommand(opts *options) *cobra.Command {
	command := &cobra.Command{
		Use:     "zipcopy",
		Aliases: []string{"zcopy", "zcp", "zp"},
		Short:   "copy and zip batches",
		Example: "fassst zcopy path/to/source path/to/target",
		Args:    cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			fmt.Println("cmd version:", Version)
			fmt.Println("pkg version:", fs.Version)
		},
	}

	return command
}
