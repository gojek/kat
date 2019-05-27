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
	describeCmd.PersistentFlags().StringP("topic-name", "t", "", "Topic name to describe")
}

func describe(cmd *cobra.Command, args []string) {
	admin := utils.GetAdminClient(cmd)
	topic := getTopicName(cmd)

	topicMetadata, err := admin.DescribeTopics([]string{topic})
	if err != nil {
		fmt.Printf("Error while retrieving topic information %v\n", err)
		return
	}

	for metadata := range topicMetadata {
		data := *topicMetadata[metadata]
		fmt.Printf("Name: %v,\nIsInternal: %v,\nPartitions:\n", data.Name, data.IsInternal)

		partitions := data.Partitions
		for partition := range partitions {
			partitionMetadata := *partitions[partition]
			fmt.Printf("Id: %v, Leader: %v, Replicas: %v, ISR: %v, OfflineReplicas: %v\n", partitionMetadata.ID, partitionMetadata.Leader, partitionMetadata.Replicas, partitionMetadata.Isr, partitionMetadata.OfflineReplicas)
		}
	}
}
