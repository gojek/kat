package describe

import (
	"fmt"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/cmd/base"

	"github.com/gojek/kat/logger"
	"github.com/spf13/cobra"
)

type describeTopic struct {
	client.Describer
	topics []string
}

var DescribeTopicCmd = &cobra.Command{
	Use:   "describe",
	Short: "Describes the given topic",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		d := describeTopic{Describer: base.Init(cobraUtil).GetTopic(), topics: cobraUtil.GetTopicNames()}
		d.describeTopic()
	},
}

func init() {
	DescribeTopicCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	if err := DescribeTopicCmd.MarkPersistentFlagRequired("topics"); err != nil {
		logger.Fatal(err)
	}
}

func (d *describeTopic) describeTopic() {
	metadata, err := d.Describe(d.topics)
	if err != nil {
		logger.Fatalf("Error while retrieving topic metadata - %v\n", err)
	}
	printConfigs(metadata)
}

func printConfigs(metadata []*client.TopicMetadata) {
	for _, topicMetadata := range metadata {
		fmt.Printf("topic Name: %v,\nIsInternal: %v,\nPartitions:\n", topicMetadata.Name, topicMetadata.IsInternal)

		partitions := topicMetadata.Partitions
		for _, partitionMetadata := range partitions {
			fmt.Printf("Id: %v, Leader: %v, Replicas: %v, ISR: %v, OfflineReplicas: %v\n",
				partitionMetadata.ID, partitionMetadata.Leader, partitionMetadata.Replicas,
				partitionMetadata.Isr, partitionMetadata.OfflineReplicas)
		}
		fmt.Println()
	}
}
