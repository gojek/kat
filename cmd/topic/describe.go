package topic

import (
	"fmt"
	"github.com/spf13/cobra"
	"source.golabs.io/hermes/kafka-admin-tools/utils"
)

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describes the given topic",
	Run:   describe,
}

func init() {
	describeCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	describeCmd.MarkPersistentFlagRequired("topics")
}

func describe(cmd *cobra.Command, args []string) {
	admin := utils.GetAdminClient(cmd)
	topics := getTopicNames(cmd)

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
