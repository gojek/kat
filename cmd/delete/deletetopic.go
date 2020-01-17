package delete

import (
	"fmt"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/pkg/model"

	"github.com/gojek/kat/cmd/base"

	"github.com/gojek/kat/logger"
	"github.com/gojek/kat/ui"
	"github.com/kevinburke/ssh_config"
	"github.com/spf13/cobra"
)

type deleteTopic struct {
	client.Lister
	client.Deleter
	lastWrite      int64
	dataDir        string
	topicWhitelist string
	topicBlacklist string
	sshPort        string
	sshKeyFilePath string
	userInput      userInput
}

type userInput interface {
	AskForConfirmation(string) bool
}

var DeleteTopicCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete the topics satisfying the passed criteria if any",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		lastWrite := int64(cobraUtil.GetIntArg("last-write"))
		var baseCmd *base.Cmd
		if lastWrite == 0 {
			baseCmd = base.Init(cobraUtil)
		} else {
			baseCmd = base.Init(cobraUtil, base.WithSSH())
		}
		d := deleteTopic{
			Lister:         baseCmd.GetTopic(),
			Deleter:        baseCmd.GetTopic(),
			lastWrite:      lastWrite,
			dataDir:        cobraUtil.GetStringArg("data-dir"),
			topicWhitelist: cobraUtil.GetStringArg("topic-whitelist"),
			topicBlacklist: cobraUtil.GetStringArg("topic-blacklist"),
			sshPort:        cobraUtil.GetStringArg("ssh-port"),
			sshKeyFilePath: cobraUtil.GetStringArg("ssh-key-file-path"),
			userInput:      &ui.UserInput{},
		}
		d.deleteTopic()
	},
}

func init() {
	DeleteTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	DeleteTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs")
	DeleteTopicCmd.PersistentFlags().StringP("topic-whitelist", "", "", "Regex pattern to include topics")
	DeleteTopicCmd.PersistentFlags().StringP("topic-blacklist", "", "", "Regex pattern to exclude topics")
	DeleteTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	DeleteTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
}

func (d *deleteTopic) deleteTopic() {
	regex, include, err := d.filterCriteria()
	if err != nil {
		logger.Fatal(err)
	}
	topics, err := d.getTopics(regex, include)
	if err != nil {
		logger.Fatal(err)
	}
	if len(topics) == 0 {
		return
	}
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
	confirmDelete := d.userInput.AskForConfirmation("Do you really want to delete the above topics?")
	if confirmDelete {
		err = d.Delete(topics)
		if err != nil {
			logger.Fatalf("Error while deleting topics - %v\n", err)
		}
	}
}

func (d *deleteTopic) filterCriteria() (regex string, include bool, err error) {
	if ((d.topicWhitelist == "") && (d.topicBlacklist == "")) || ((d.topicWhitelist != "") && (d.topicBlacklist != "")) {
		return regex, include, fmt.Errorf("any one of blacklist or whitelist should be passed")
	}
	if d.topicWhitelist != "" {
		include = true
		regex = d.topicWhitelist
	} else if d.topicBlacklist != "" {
		include = false
		regex = d.topicBlacklist
	}
	return regex, include, nil
}

func (d *deleteTopic) getLastWrittenTopics() ([]string, error) {
	topics, err := d.ListLastWrittenTopics(d.lastWrite, d.dataDir)
	if err != nil {
		logger.Errorf("Error while fetching topic list - %v\n", err)
		return nil, err
	}
	return topics, nil
}

func (d *deleteTopic) getTopics(regex string, include bool) ([]string, error) {
	if d.lastWrite != 0 {
		lastWrittenTopics, err := d.getLastWrittenTopics()
		if err != nil {
			return nil, err
		}
		topics, err := model.ListUtil{List: lastWrittenTopics}.Filter(regex, include)
		if err != nil {
			logger.Errorf("Error while fetching topic list - %v\n", err)
			return nil, err
		}
		return topics, nil
	}

	topics, err := d.ListOnly(regex, include)
	if err != nil {
		logger.Errorf("Error while fetching topic list - %v\n", err)
		return nil, err
	}
	return topics, err
}
