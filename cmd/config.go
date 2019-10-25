package cmd

import (
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Config of topics",
}

func init() {
	configCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names for which retention needs to be changed")
	configCmd.MarkPersistentFlagRequired("topics")

	configCmd.AddCommand(showConfigCmd)
	configCmd.AddCommand(alterConfigCmd)
}
