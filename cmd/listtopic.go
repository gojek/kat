package cmd

import (
	"fmt"

	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/util"
	"github.com/kevinburke/ssh_config"
	"github.com/spf13/cobra"
)

type listTopic struct {
	BaseCmd
	replicationFactor int
	lastWrite         int64
	dataDir           string
}

var listTopicCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := util.NewCobraUtil(command)
		lastWrite := int64(cobraUtil.GetIntArg("last-write"))
		var baseCmd BaseCmd
		if lastWrite == 0 {
			baseCmd = Init(cobraUtil)
		} else {
			baseCmd = Init(cobraUtil, WithSSH())
		}

		l := listTopic{
			BaseCmd:           baseCmd,
			replicationFactor: cobraUtil.GetIntArg("replication-factor"),
			lastWrite:         lastWrite,
			dataDir:           cobraUtil.GetStringArg("data-dir"),
		}
		l.listTopic()
	},
}

func init() {
	listTopicCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
	listTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	listTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs")
	listTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	listTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
}

func (l *listTopic) listTopic() {
	if l.lastWrite != 0 {
		l.listLastWrittenTopics()
	} else {
		topicDetails, err := l.TopicCli.List()
		if err != nil {
			logger.Fatalf("Error while fetching topic list - %v\n", err)
		}
		if len(topicDetails) == 0 {
			logger.Info("No topics found.")
			return
		}
		var topics []string
		for topicDetail := range topicDetails {
			if l.replicationFactor == 0 {
				topics = append(topics, topicDetail)
			} else {
				if int(topicDetails[topicDetail].ReplicationFactor) == l.replicationFactor {
					topics = append(topics, topicDetail)
				}
			}
		}
		printTopics(topics)
	}
}

func (l *listTopic) listLastWrittenTopics() {
	topics, err := l.TopicCli.ListLastWrittenTopics(l.lastWrite, l.dataDir)
	if err != nil {
		logger.Errorf("Error while fetching topic list - %v\n", err)
		return
	}
	if len(topics) == 0 {
		logger.Info("No topics found.")
		return
	}
	printTopics(topics)
	return
}

func printTopics(topics []string) {
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
}
