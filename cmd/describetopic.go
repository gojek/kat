package cmd

import (
	"fmt"
	"github.com/gojekfarm/kat/pkg"

	"github.com/spf13/cobra"
)

type describeTopic struct {
	topics []string
}

var describeTopicCmd = &cobra.Command{
	Use:    "describe",
	Short:  "Describes the given topic",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		d := describeTopic{topics: Cobra.GetTopicNames()}
		d.describeTopic()
	},
	PostRun: clearTopicCli,
}

func init() {
	describeTopicCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	describeTopicCmd.MarkPersistentFlagRequired("topics")
}

func (d *describeTopic) describeTopic() {
	metadata, err := TopicCli.Describe(d.topics)
	if err != nil {
		fmt.Printf("Error while retrieving topic metadata - %v\n", err)
		return
	}
	printConfigs(metadata)
}

func printConfigs(metadata []*pkg.TopicMetadata) {
	for _, topicMetadata := range metadata {
		fmt.Printf("Topic Name: %v,\nIsInternal: %v,\nPartitions:\n", (*topicMetadata).Name, (*topicMetadata).IsInternal)

		partitions := (*topicMetadata).Partitions
		for _, partitionMetadata := range partitions {
			fmt.Printf("Id: %v, Leader: %v, Replicas: %v, ISR: %v, OfflineReplicas: %v\n", (*partitionMetadata).ID, (*partitionMetadata).Leader, (*partitionMetadata).Replicas, (*partitionMetadata).Isr, (*partitionMetadata).OfflineReplicas)
		}
		fmt.Println()
	}
}
