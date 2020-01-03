package pkg

import (
	"bytes"
	"fmt"
	"golang.org/x/crypto/ssh"
	"strconv"
	"strings"
	"time"
)

type sshCli interface {
	Dial(address string) (*ssh.Client, error)
	Execute(client *ssh.Client, cmd string) (*bytes.Buffer, error)
}

type KafkaRemoteClient struct {
	KafkaApiClient
	sshCli
}

const DEFAULT_REGEX = ".*\\(offsets\\|properties\\)$"
const FIND_LAST_WRITTEN_DIRECTORIES = "find %s -maxdepth 1 -not -path \"*/\\.*\" -not -newermt \"%s\""
const REMOVE_PATH_PREFIX = "xargs -I{} echo {} | rev | cut -d / -f1 | rev"
const REMOVE_PARTITION_SUFFIX = "xargs -I{} echo {} | rev | cut -d - -f2- | rev"
const SORT_AND_COUNT = "sort | uniq -c"
const EXCLUDE_REGEX = "grep -v \"%s\""
const REORDER = "awk '{ print $2 \" \" $1}'"

func NewKafkaSshCli(apiClient KafkaApiClient, user, port, keyfile string) (KafkaSshClient, error) {
	sshClient, err := NewSshClient(user, port, keyfile)
	if err != nil {
		return nil, err
	}
	return &KafkaRemoteClient{apiClient, sshClient}, nil
}

func (s *KafkaRemoteClient) ListTopics(request ListTopicsRequest) ([]string, error) {
	brokers := s.ListBrokers()
	dateTime := time.Unix(request.LastWritten, 0)
	topicMap := make(map[string]int)
	for id := 1; id <= len(brokers); id++ {
		fmt.Printf("Sshing into broker - %v\n", brokers[id])
		client, err := s.sshCli.Dial(strings.Split(brokers[id], ":")[0])
		if err != nil {
			fmt.Printf("Error while dialing ssh session - %v\n", err)
			return nil, err
		}

		_, err = s.sshCli.Execute(client, fmt.Sprintf("cd %s", "/data/kafksdfa-logs"))
		if err != nil {
			fmt.Printf("Invalid data directory - %v\n", err)
			return nil, err
		}

		cmd := fmt.Sprintf("%s | %s | %s | %s | %s | %s", fmt.Sprintf(FIND_LAST_WRITTEN_DIRECTORIES, "/data/kafka-logs", dateTime.UTC().Format(time.UnixDate)), REMOVE_PATH_PREFIX, REMOVE_PARTITION_SUFFIX, SORT_AND_COUNT, fmt.Sprintf(EXCLUDE_REGEX, DEFAULT_REGEX), REORDER)
		data, err := s.sshCli.Execute(client, cmd)
		if err != nil {
			fmt.Printf("Error while executing remote command - %v\n", err)
			return nil, err
		}

		topics := strings.Split(data.String(), "\n")
		err = s.mapTopics(topicMap, topics)
		if err != nil {
			fmt.Printf("Error while reading topics in broker %v - %v\n", id, err)
			return nil, err
		}
	}

	topics, e := s.getFullyStaleTopics(topicMap)
	return topics, e
}

func (s *KafkaRemoteClient) mapTopics(topicMap map[string]int, topics []string) error {
	for _, topic := range topics {
		if topic == "" {
			continue
		}
		detail := strings.Split(topic, " ")
		i, err := strconv.Atoi(detail[1])
		if err != nil {
			return err
		}
		val, _ := topicMap[detail[0]]
		topicMap[detail[0]] = val + i
	}
	return nil
}

func (s *KafkaRemoteClient) getFullyStaleTopics(topicMap map[string]int) ([]string, error) {
	var staleTopics []string
	topicDetails, err := s.ListTopicDetails()
	if err != nil {
		fmt.Printf("Error while fetching topic details - %v\n", err)
		return nil, err
	}

	for topic := range topicMap {
		detail, ok := topicDetails[topic]
		if !ok {
			fmt.Printf("Topic cannot be processed as it is not returned by the list api %v - %v\n", topic, err)
			continue
		}

		// Mark a topic unused, only if all the partitions are last written before the time specified
		if topicMap[topic] != (int(detail.NumPartitions) * int(detail.ReplicationFactor)) {
			continue
		}

		staleTopics = append(staleTopics, topic)
	}
	return staleTopics, nil
}
