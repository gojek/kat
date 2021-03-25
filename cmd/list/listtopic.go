package list

import (
	"fmt"
	"sync"

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
	size              int64
}

var ListTopicCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists the topics satisfying the passed criteria if any",
	Run: func(command *cobra.Command, args []string) {
		cobraUtil := base.NewCobraUtil(command)
		lastWrite := int64(cobraUtil.GetIntArg("last-write"))
		size := int64(cobraUtil.GetIntArg("size"))
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
			size:              size,
		}
		l.listTopic()
	},
}

func init() {
	ListTopicCmd.PersistentFlags().IntP("replication-factor", "r", 0, "Replication Factor of the topic")
	ListTopicCmd.PersistentFlags().Int64P("last-write", "l", 0, "Last write time for topics in epoch format")
	// tododata directory can be fetched with describeLogDirs request making the parameter redundant
	ListTopicCmd.PersistentFlags().StringP("data-dir", "d", "/var/log/kafka", "Data directory for kafka logs")
	ListTopicCmd.PersistentFlags().StringP("ssh-port", "p", ssh_config.Default("Port"), "Ssh port on the kafka brokers")
	ListTopicCmd.PersistentFlags().StringP("ssh-key-file-path", "k", "~/.ssh/id_rsa", "Path to ssh key file")
	ListTopicCmd.PersistentFlags().Int64P("size", "s", -1, "size less than or equal to specified in bytes")
}

func (l *listTopic) listTopic() {
	topics, err := l.getTopicsFilteredByFlags()
	if err != nil {
		logger.Fatalf("Error while fetching topic list - %v\n", err)
		return
	}
	if len(topics) == 0 {
		logger.Info("No topics found")
		return
	}
	printTopics(topics)
}

func (l *listTopic) getTopicsFilteredByFlags() ([]string, error) {
	topicsChannel := make(chan string)
	errorChannel := make(chan error)
	lastWrittenTopicsChannel := make(chan string)
	sizeFilteredTopicsChannel := make(chan string)
	waitChannel := make(chan interface{})
	var wg sync.WaitGroup
	wg.Add(1)
	go l.listAllTopics(topicsChannel, errorChannel, &wg)
	if l.lastWrite != 0 {
		wg.Add(1)
		go l.listLastWrittenTopics(lastWrittenTopicsChannel, errorChannel, &wg)
	}
	if l.size >= 0 {
		wg.Add(1)
		go l.listTopicWithSizeFilter(sizeFilteredTopicsChannel, errorChannel, &wg)
	}
	go func() {
		wg.Wait()
		close(waitChannel)
	}()
	select {
	case err := <-errorChannel:
		return nil, err
	case <-waitChannel:
		topics := getListFromChannel(topicsChannel)
		if l.lastWrite != 0 {
			lastWrittenTopics := getListFromChannel(lastWrittenTopicsChannel)
			topics = getCommonElements(topics, lastWrittenTopics)
		}
		if l.size >= 0 {
			sizeFilteredTopics := getListFromChannel(sizeFilteredTopicsChannel)
			topics = getCommonElements(topics, sizeFilteredTopics)
		}
		return topics, nil
	}
}

func getListFromChannel(channel chan string) []string {
	list := make([]string, 0)
	for element := range channel {
		list = append(list, element)
	}
	return list
}

func (l *listTopic) listAllTopics(topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	topicDetails, err := l.List()
	if err != nil {
		errorChannel <- err
		return
	}
	wg.Done()
	for topicDetail := range topicDetails {
		if l.replicationFactor == 0 {
			topicsChannel <- topicDetail
		} else if int(topicDetails[topicDetail].ReplicationFactor) == l.replicationFactor {
			topicsChannel <- topicDetail
		}
	}
	close(topicsChannel)
}

func (l *listTopic) listLastWrittenTopics(topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	topics, err := l.ListLastWrittenTopics(l.lastWrite, l.dataDir)
	if err != nil {
		errorChannel <- err
		return
	}
	wg.Done()
	for _, v := range topics {
		topicsChannel <- v
	}
	close(topicsChannel)
}

func (l *listTopic) listTopicWithSizeFilter(topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	topics, err := l.ListTopicWithSizeLessThanOrEqualTo(l.size)
	if err != nil {
		errorChannel <- err
		return
	}
	wg.Done()
	for _, topic := range topics {
		topicsChannel <- topic
	}
	close(topicsChannel)
}

func getCommonElements(list1, list2 []string) []string {
	intersectionList := make([]string, 0)
	for _, element1 := range list1 {
		for _, element2 := range list2 {
			if element1 == element2 {
				intersectionList = append(intersectionList, element1)
			}
		}
	}
	return intersectionList
}

func printTopics(topics []string) {
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
}
