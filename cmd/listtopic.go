package cmd

import (
	"fmt"
	"github.com/gojekfarm/kat/pkg"
	"github.com/kevinburke/ssh_config"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

type listTopic struct {
	replicationFactor int
	lastWrite         int64
	dataDir           string
	sshPort           string
	sshKeyFilePath    string
}

var listTopicCmd = &cobra.Command{
	Use:    "list",
	Short:  "Lists the topics satisfying the passed criteria if any",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		l := listTopic{
			replicationFactor: Cobra.GetIntArg("replication-factor"),
			lastWrite:         int64(Cobra.GetIntArg("last-write")),
			dataDir:           Cobra.GetCmdArg("data-dir"),
			sshPort:           Cobra.GetCmdArg("ssh-port"),
			sshKeyFilePath:    Cobra.GetCmdArg("ssh-key-file-path"),
		}
		l.listTopic()
	},
	PostRun: clearTopicCli,
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
}

func (l *listTopic) listLastWrittenTopics() {
	var err error
	keyfile, _ := homedir.Expand(l.sshKeyFilePath)
	TopicCli, err = pkg.NewTopic(pkg.NewSaramaClient(Cobra.GetSaramaClient("broker-list")),
		pkg.WithSSHClient(ssh_config.Get("*", "User"), l.sshPort, keyfile))
	if err != nil {
		fmt.Printf("Error while creating kafka client - %v\n", err)
		return
	}

	brokers, err := TopicCli.ListLastWrittenTopics(l.lastWrite, l.dataDir)
	if err != nil {
		fmt.Printf("Error while fetching topic list - %v\n", err)
		return
	}
	fmt.Println(brokers)
	return
}
