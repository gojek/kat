package topic

import (
	"github.com/gojekfarm/kat/cmd/topic/config"
	"github.com/spf13/cobra"
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
