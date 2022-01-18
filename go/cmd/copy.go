package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	fst "fassst/pkg/fassst"
	pkgfs "fassst/pkg/fs"
)

type copyOptions struct {
	*options

	source string
	target string
}

func NewCopyCommand(opts *options) *cobra.Command {
	var copyOpts copyOptions
	copyOpts.options = opts
	command := &cobra.Command{
		Use:     "copy",
		Aliases: []string{"cp", "get"},
		Short:   "copy from source to target",
		Example: "fassst copy path/to/source path/to/target",
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 2 {
				copyOpts.target = pkgfs.MakeSureHasSuffix(args[1], "/")
			} else {
				copyOpts.target = "./"
			}
			copyOpts.source = pkgfs.MakeSureHasSuffix(args[0], "/")
			return copyOpts.run()
		},
	}

	return command
}

func (o copyOptions) run() error {
	url, fs, err := pkgfs.FileSystemByUrl(o.source)
	if err != nil {
		return fmt.Errorf("source file system from url: %w", err)
	}
	urlTarget, fsTarget, err := pkgfs.FileSystemByUrl(o.target)
	if err != nil {
		return fmt.Errorf("target file system from url: %w", err)
	}
	o.log.Info("copying...")
	fst.Copy(fs, fsTarget, url, urlTarget, o.maxGoroutines, o.log)
	return nil
}
