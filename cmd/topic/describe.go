package topic

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/topicutil"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
)

type describe struct {
	admin  sarama.ClusterAdmin
	topics []string
}

var describeCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describes the given topic",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		d := describe{admin: u.GetAdminClient("broker-list"), topics: u.GetTopicNames()}
		d.describe()
	},
}

func init() {
	describeCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	describeCmd.MarkPersistentFlagRequired("topics")
}

func (d *describe) describe() {
	metadata := topicutil.DescribeTopicMetadata(d.admin, d.topics)
	if metadata == nil {
		return
	}
	printConfigs(metadata)
}

func printConfigs(metadata []*sarama.TopicMetadata) {
	for _, topicMetadata := range metadata {
		fmt.Printf("Topic Name: %v,\nIsInternal: %v,\nPartitions:\n", (*topicMetadata).Name, (*topicMetadata).IsInternal)

		partitions := (*topicMetadata).Partitions
		for _, partitionMetadata := range partitions {
			fmt.Printf("Id: %v, Leader: %v, Replicas: %v, ISR: %v, OfflineReplicas: %v\n", (*partitionMetadata).ID, (*partitionMetadata).Leader, (*partitionMetadata).Replicas, (*partitionMetadata).Isr, (*partitionMetadata).OfflineReplicas)
		}
		fmt.Println()
	}
}
