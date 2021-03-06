package list

import (
	"context"
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
	ListTopicCmd.PersistentFlags().Int64P("size", "s", -1,
		"Size less than or equal to specified in bytes. Compares the true size utilized by topic on disk. ie dataProduced*replicationFactor")
}

func (l *listTopic) listTopic() {
	topics, err := l.getTopicsFilteredByFlags()
	if err != nil {
		logger.Fatalf("Error while fetching topic list - %v\n", err)
	}
	if len(topics) == 0 {
		logger.Info("No topics found")
		return
	}
	printTopics(topics)
}

func (l *listTopic) getTopicsFilteredByFlags() ([]string, error) {
	topicsChannel := make(chan string)
	errorChannel := make(chan error, 3)
	lastWrittenTopicsChannel := make(chan string)
	sizeFilteredTopicsChannel := make(chan string)
	ctx, cancelFunc := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(3)
	go l.listAllTopics(ctx, cancelFunc, topicsChannel, errorChannel, &wg)
	go l.listLastWrittenTopics(ctx, cancelFunc, topicsChannel, lastWrittenTopicsChannel, errorChannel, &wg)
	go l.listTopicWithSizeFilter(ctx, cancelFunc, lastWrittenTopicsChannel, sizeFilteredTopicsChannel, errorChannel, &wg)
	wg.Wait()
	select {
	case err := <-errorChannel:
		return nil, err
	default:
		return getListFromChannel(sizeFilteredTopicsChannel), nil
	}
}

func getListFromChannel(channel chan string) []string {
	list := make([]string, 0)
	for element := range channel {
		list = append(list, element)
	}
	return list
}

func (l *listTopic) listAllTopics(ctx context.Context, cancelFunc context.CancelFunc, topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	defer close(topicsChannel)
	topicDetails, err := l.List()
	wg.Done()
	select {
	case <-ctx.Done():
		return
	default:
		if err != nil {
			cancelFunc()
			errorChannel <- err
			return
		}
		processTopicsByReplicationFactor(ctx, l.replicationFactor, topicsChannel, topicDetails)
	}
}

func processTopicsByReplicationFactor(ctx context.Context, replicationFactor int, topicsChannel chan string, topicDetails map[string]client.TopicDetail) {
	for topicDetail := range topicDetails {
		if replicationFactor == 0 {
			select {
			case <-ctx.Done():
				return
			case topicsChannel <- topicDetail:
			}
		} else if int(topicDetails[topicDetail].ReplicationFactor) == replicationFactor {
			select {
			case <-ctx.Done():
				return
			case topicsChannel <- topicDetail:
			}
		}
	}
}

func (l *listTopic) listLastWrittenTopics(ctx context.Context, cancelFunc context.CancelFunc,
	inputChannel, topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	defer close(topicsChannel)
	if l.lastWrite == 0 {
		wg.Done()
		for topic := range inputChannel {
			topicsChannel <- topic
		}
		return
	}
	topics, err := l.ListLastWrittenTopics(l.lastWrite, l.dataDir)
	wg.Done()
	select {
	case <-ctx.Done():
		return
	default:
		if err != nil {
			cancelFunc()
			errorChannel <- err
			return
		}
		findCommonElement(ctx, topicsChannel, inputChannel, topics)
		return
	}
}

func (l *listTopic) listTopicWithSizeFilter(ctx context.Context, cancelFunc context.CancelFunc,
	inputChannel, topicsChannel chan string, errorChannel chan error, wg *sync.WaitGroup) {
	defer close(topicsChannel)
	if l.size < 0 {
		wg.Done()
		for topic := range inputChannel {
			topicsChannel <- topic
		}
		return
	}

	topics, err := l.ListTopicWithSizeLessThanOrEqualTo(l.size)
	wg.Done()
	select {
	case <-ctx.Done():
		return
	default:
		if err != nil {
			cancelFunc()
			errorChannel <- err
			return
		}
		findCommonElement(ctx, topicsChannel, inputChannel, topics)
		return
	}
}

func findCommonElement(ctx context.Context, topicsChannel, inputChannel chan string, topics []string) {
	for inputTopic := range inputChannel {
		for _, topic := range topics {
			if inputTopic == topic {
				select {
				case <-ctx.Done():
					return
				case topicsChannel <- topic:
				}
			}
		}
	}
}

func printTopics(topics []string) {
	fmt.Println("------------------------------------------------------------")
	for _, topic := range topics {
		fmt.Println(topic)
	}
	fmt.Println("------------------------------------------------------------")
}
