package list

import (
	"fmt"
	"time"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/cmd/base"

	"github.com/gojek/kat/logger"
	"github.com/kevinburke/ssh_config"
	"github.com/spf13/cobra"
)

const defaultLastWrittenTime int64 = 2 * 7 * 24 * 60 * 60

type listTopic struct {
	client.Lister
	replicationFactor int
	lastWrite         int64
	dataDir           string
	isEmpty           bool
}

var ListTopicCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		isEmpty := cobraUtil.GetBoolArg("empty")
		lastWrite := getLastWrite(int64(cobraUtil.GetIntArg("last-write")), isEmpty)
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
			isEmpty:           isEmpty,
		}
		l.listTopic()
	},
}

func init() {
	ListTopicCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
	ListTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	ListTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs") // data directory can be fetched with describeLogDirs request making the parameter redundant
	ListTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	ListTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
	ListTopicCmd.PersistentFlags().BoolP("empty", "i", false, "Return only empty topics")
}

func getLastWrite(lastWrite int64, isEmpty bool) int64 {
	if isEmpty && lastWrite == 0 {
		return time.Now().Unix() - defaultLastWrittenTime
	}
	return lastWrite
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
	topics, err := l.filterEmptyTopicsIfNeeded()
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

func (l *listTopic) filterEmptyTopicsIfNeeded() ([]string, error) {
	if l.isEmpty {
		return l.ListEmptyLastWrittenTopics(l.lastWrite, l.dataDir)
	}

	return l.ListLastWrittenTopics(l.lastWrite, l.dataDir)
}

func printTopics(topics []string) {
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
}
