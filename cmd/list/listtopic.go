package list

import (
	"fmt"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/cmd/base"

	"github.com/gojek/kat/logger"
	"github.com/kevinburke/ssh_config"
	"github.com/spf13/cobra"
)

type listTopic struct {
	client.Lister
	replicationFactor int
	lastWrite         int64
	dataDir           string
}

var ListTopicCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		lastWrite := int64(cobraUtil.GetIntArg("last-write"))
		var baseCmd *base.Cmd
		if lastWrite == 0 {
			baseCmd = base.Init(cobraUtil)
		} else {
			baseCmd = base.Init(cobraUtil, base.WithSSH())
		}

		l := listTopic{
			Lister:            baseCmd.GetTopic(),
			replicationFactor: cobraUtil.GetIntArg("replication-factor"),
			lastWrite:         lastWrite,
			dataDir:           cobraUtil.GetStringArg("data-dir"),
		}
		l.listTopic()
	},
}

func init() {
	ListTopicCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
	ListTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	ListTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs")
	ListTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	ListTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
}

func (l *listTopic) listTopic() {
	if l.lastWrite != 0 {
		err := l.listLastWrittenTopics()
		if err != nil {
			logger.Fatal(err)
		}
	} else {
		topicDetails, err := l.List()
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
			} else if int(topicDetails[topicDetail].ReplicationFactor) == l.replicationFactor {
				topics = append(topics, topicDetail)
			}
		}
		printTopics(topics)
	}
}

func (l *listTopic) listLastWrittenTopics() error {
	topics, err := l.ListLastWrittenTopics(l.lastWrite, l.dataDir)
	if err != nil {
		logger.Errorf("Error while fetching topic list - %v\n", err)
		return err
	}
	if len(topics) == 0 {
		logger.Info("No topics found.")
	} else {
		printTopics(topics)
	}
	return nil
}

func printTopics(topics []string) {
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
}
