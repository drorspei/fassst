package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"
)

const mb = 1024 * 1024

type zipcopyOptions struct {
	*options

	source string
	target string

	maxBatchCount int
	maxBatchSize  int
}

func NewZipCopyCommand(opts *options) *cobra.Command {
	var zipcopyOpts zipcopyOptions
	zipcopyOpts.options = opts
	command := &cobra.Command{
		Use:     "zipcopy",
		Aliases: []string{"zcopy", "zcp", "zp"},
		Short:   "copy and zip batches",
		Example: "fassst zcopy path/to/source path/to/target",
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 2 {
				zipcopyOpts.target = pkgfs.MakeSureHasSuffix(args[1], "/")
			} else {
				zipcopyOpts.target = "."
			}
			zipcopyOpts.source = pkgfs.MakeSureHasSuffix(args[0], "/")
			return zipcopyOpts.run()
		},
	}

	command.Flags().IntVarP(&zipcopyOpts.maxBatchCount,
		"max-batch-count",
		"c",
		1000,
		"maximum amount of files per archive",
	)

	command.Flags().IntVarP(&zipcopyOpts.maxBatchSize,
		"max-batch-size",
		"s",
		200,
		"maximum amount of files per archive(in MB)",
	)

	return command
}

func (o zipcopyOptions) run() error {
	url, fs, err := pkgfs.FileSystemByUrl(o.source)
	if err != nil {
		return fmt.Errorf("source file system from url: %w", err)
	}
	urlTarget, fsTarget, err := pkgfs.FileSystemByUrl(o.target)
	if err != nil {
		return fmt.Errorf("target file system from url: %w", err)
	}

	fst.Copy(fs, fsTarget, url, urlTarget, o.maxGoroutines)
	return nil
}
