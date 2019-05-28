package topic

import (
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kafka-admin-tools/utils"
	"strings"
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

}

func getTopicNames(cmd *cobra.Command) []string {
	return strings.Split(utils.GetCmdArg(cmd, "topics"), ",")
}