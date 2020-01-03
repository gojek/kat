package cmd

import (
	"fmt"
	"github.com/gojekfarm/kat/pkg"
	"github.com/gojekfarm/kat/util"
	"github.com/kevinburke/ssh_config"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
)

type deleteTopic struct {
	lastWrite      int64
	dataDir        string
	topicWhitelist string
	topicBlacklist string
	sshPort        string
	sshKeyFilePath string
	io             io
}

type io interface {
	AskForConfirmation(string) bool
}

var deleteTopicCmd = &cobra.Command{
	Use:    "delete",
	Short:  "Delete the topics satisfying the passed criteria if any",
	PreRun: loadTopicCli,
	Run: func(command *cobra.Command, args []string) {
		d := deleteTopic{
			lastWrite:      int64(Cobra.GetIntArg("last-write")),
			dataDir:        Cobra.GetCmdArg("data-dir"),
			topicWhitelist: Cobra.GetCmdArg("topic-whitelist"),
			topicBlacklist: Cobra.GetCmdArg("topic-blacklist"),
			sshPort:        Cobra.GetCmdArg("ssh-port"),
			sshKeyFilePath: Cobra.GetCmdArg("ssh-key-file-path"),
			io:             &util.IO{},
		}
		err := d.initTopicCliWithSshClient()
		if err != nil {
			return
		}
		d.deleteTopic()
	},
	PostRun: clearTopicCli,
}

func init() {
	deleteTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	deleteTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs")
	deleteTopicCmd.PersistentFlags().StringP("topic-whitelist", "", "", "Regex pattern to include topics")
	deleteTopicCmd.PersistentFlags().StringP("topic-blacklist", "", "", "Regex pattern to exclude topics")
	deleteTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	deleteTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
}

func (d *deleteTopic) deleteTopic() {
	var regex string
	var include bool
	if ((d.topicWhitelist == "") && (d.topicBlacklist == "")) || ((d.topicWhitelist != "") && (d.topicBlacklist != "")) {
		fmt.Printf("Any one of blacklist or whitelist should be passed.")
		return
	}
	if d.topicWhitelist != "" {
		include = true
		regex = d.topicWhitelist
	} else if d.topicBlacklist != "" {
		include = false
		regex = d.topicBlacklist
	}
	topics, err := d.getTopics(regex, include)
	if err != nil || len(topics) == 0 {
		return
	}
	for _, topic := range topics {
		fmt.Println(topic)
	}
	confirmDelete := d.io.AskForConfirmation("Do you really want to delete the above topics?")
	if confirmDelete {
		err = TopicCli.Delete(topics)
		if err != nil {
			fmt.Printf("Error while deleting topics - %v\n", err)
		}
	}
}

func (d *deleteTopic) getLastWrittenTopics() ([]string, error) {
	topics, err := TopicCli.ListLastWrittenTopics(d.lastWrite, d.dataDir)
	if err != nil {
		fmt.Printf("Error while fetching topic list - %v\n", err)
		return nil, err
	}
	return topics, nil
}

func (d *deleteTopic) getTopics(regex string, include bool) ([]string, error) {
	var topics []string
	var err error
	if d.lastWrite != 0 {
		lastWrittenTopics, err := d.getLastWrittenTopics()
		if err != nil {
			return nil, err
		}
		topics, err = util.Filter(lastWrittenTopics, regex, include)
	} else {
		topics, err = TopicCli.ListOnly(regex, include)
	}
	if err != nil {
		fmt.Printf("Error while fetching topic list - %v\n", err)
		return nil, err
	}
	return topics, err
}

func (d *deleteTopic) initTopicCliWithSshClient() error {
	keyFile, err := homedir.Expand(d.sshKeyFilePath)
	if err != nil {
		fmt.Printf("Error while resolving data directory - %v\n", err)
		return err
	}
	TopicCli, err = pkg.NewTopic(pkg.NewSaramaClient(Cobra.GetSaramaClient("broker-list")),
		pkg.WithSshClient(ssh_config.Get("*", "User"), d.sshPort, keyFile))
	if err != nil {
		fmt.Printf("Error while creating kafka client - %v\n", err)
	}
	return err
}
