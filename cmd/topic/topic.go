package topic

import (
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kat/cmd/topic/config"
)

var TopicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Admin commands on topics",
}

func init() {
	TopicCmd.PersistentFlags().StringP("broker-list", "b", "", "Comma separated list of broker ips")
	TopicCmd.MarkPersistentFlagRequired("broker-list")

	TopicCmd.AddCommand(listCmd)
	TopicCmd.AddCommand(describeCmd)
	TopicCmd.AddCommand(increaseReplicationFactorCmd)
	TopicCmd.AddCommand(config.ConfigCmd)

}
