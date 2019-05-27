package topic

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
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

func getReplicationFactor(cmd *cobra.Command) int {
	flags := cmd.Flags()
	replicationFactor, err := flags.GetInt("replication-factor")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return replicationFactor
}

func getTopicNames(cmd *cobra.Command) []string {
	flags := cmd.Flags()
	topics, err := flags.GetString("topics")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.Split(topics, ",")
}

func getNumOfBrokers(cmd *cobra.Command) int {
	flags := cmd.Flags()
	numOfBrokers, err := flags.GetInt("num-of-brokers")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return numOfBrokers
}