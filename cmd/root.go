package cmd

import (
	"fmt"
	"os"

	"github.com/gojek/kat/cmd/list"
	"github.com/gojek/kat/cmd/mirror"

	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

var cliCmd = &cobra.Command{
	Use:     "kat",
	Short:   "Tool used for admin activities against specified kafka brokers",
	Version: fmt.Sprintf("%s (Commit: %s)", "0.0.1", "n/a"),
}

func init() {
	cobra.OnInitialize()
	cliCmd.AddCommand(topicCmd)
	cliCmd.AddCommand(mirror.MirrorCmd)
	cliCmd.AddCommand(list.ListConsumerGroupsCmd)
}

func Execute() {
	if err := cliCmd.Execute(); err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}
