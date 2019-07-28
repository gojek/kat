package topic

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describes the given topic",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		admin := u.GetAdminClient()
		topics := u.GetTopicNames()
		describe(admin, topics)
	},
}

func init() {
	describeCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	describeCmd.MarkPersistentFlagRequired("topics")
}

func describe(admin sarama.ClusterAdmin, topics []string) {
	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Error while retrieving topic information %v\n", err)
		return
	}

	for _, topicMetadata := range metadata {
		fmt.Printf("Topic Name: %v,\nIsInternal: %v,\nPartitions:\n", (*topicMetadata).Name, (*topicMetadata).IsInternal)

		partitions := (*topicMetadata).Partitions
		for _, partitionMetadata := range partitions {
			fmt.Printf("Id: %v, Leader: %v, Replicas: %v, ISR: %v, OfflineReplicas: %v\n", (*partitionMetadata).ID, (*partitionMetadata).Leader, (*partitionMetadata).Replicas, (*partitionMetadata).Isr, (*partitionMetadata).OfflineReplicas)
		}
		fmt.Println()
	}
}
