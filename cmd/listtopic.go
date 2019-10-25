package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

type listTopic struct {
	replicationFactor int
}

var listTopicCmd = &cobra.Command{
	Use:    "list",
	Short:  "Lists the topics satisfying the passed criteria if any",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		l := listTopic{replicationFactor: Cobra.GetIntArg("replication-factor")}
		l.listTopic()
	},
	PostRun: clearTopicCli,
}

func init() {
	listTopicCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
}

func (l *listTopic) listTopic() {
	topicDetails, err := TopicCli.List()
	if err != nil {
		fmt.Printf("Error while fetching topic list - %v\n", err)
		return
	}

	for topicDetail := range topicDetails {
		if l.replicationFactor != 0 {
			if int(topicDetails[topicDetail].ReplicationFactor) == l.replicationFactor {
				fmt.Println(topicDetail)
			}
		} else {
			fmt.Println(topicDetail)
		}
	}
}
