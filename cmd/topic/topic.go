package topic

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
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

}

func getReplicationFactor(cmd *cobra.Command) int {
	flags := cmd.Flags()
	replicationFactor, err := flags.GetInt("replication-factor")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return replicationFactor
}

func getTopicName(cmd *cobra.Command) string {
	flags := cmd.Flags()
	topicName, err := flags.GetString("topic-name")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return topicName
}
