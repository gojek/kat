package config

import (
	"github.com/spf13/cobra"
)

var ConfigCmd = &cobra.Command{
	Use:   "config",
	Short: "Config of topics",
}

func init() {
	ConfigCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names for which retention needs to be changed")
	ConfigCmd.MarkPersistentFlagRequired("topics")

	ConfigCmd.AddCommand(showCmd)
	ConfigCmd.AddCommand(alterCmd)
}
