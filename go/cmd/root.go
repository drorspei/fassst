package cmd

import (
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
)

type options struct {
	maxGoroutines int

	log *log.Logger
}

func (o options) Validate() error {
	if o.maxGoroutines < 1 {
		return fmt.Errorf("max_goroutines must be at least 1, was %d", o.maxGoroutines)
	}
	return nil
}

// NewCommand creates a new root command.
func NewCommand() *cobra.Command {
	var opts options
	var startTime time.Time
	opts.log = log.Default()
	command := &cobra.Command{
		Use:          "fassst",
		Short:        "go fassst!",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			err := opts.Validate()
			if err != nil {
				return fmt.Errorf("validate opts: %w", err)
			}
			startTime = time.Now()
			return nil
		},
		PersistentPostRun: func(_ *cobra.Command, _ []string) {
			delta := time.Since(startTime)
			opts.log.Println(delta, "fassst!")
		},
	}

	command.AddCommand(
		NewVersionCommand(),
		NewListCommand(&opts),
		NewCopyCommand(&opts),
		NewSyncCommand(&opts),
		NewZipCopyCommand(&opts),
	)

	command.PersistentFlags().IntVarP(&opts.maxGoroutines,
		"max-goroutines",
		"m",
		30,
		"number of concurrent goroutines")

	return command
}
