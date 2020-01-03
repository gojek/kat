package pkg

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type sshCli interface {
	DialAndExecute(address string, commands ...string) (*bytes.Buffer, error)
}

type KafkaRemoteClient struct {
	KafkaApiClient
	sshCli
}

const FindLastWrittenDirectories = "find %s -maxdepth 1 -not -path \"*/\\.*\" -not -newermt \"%s\""
const RemovePathPrefix = "xargs -I{} echo {} | rev | cut -d / -f1 | rev"
const RemovePartitionSuffix = "xargs -I{} echo {} | rev | cut -d - -f2- | rev"
const SortAndCount = "sort | uniq -c"
const Reorder = "awk '{ print $2 \" \" $1}'"

func NewKafkaSshCli(apiClient KafkaApiClient, user, port, keyfile string) (KafkaSshClient, error) {
	sshClient, err := NewSshClient(user, port, keyfile)
	if err != nil {
		return nil, err
	}
	return &KafkaRemoteClient{apiClient, sshClient}, nil
}

func (r *KafkaRemoteClient) ListTopics(request ListTopicsRequest) ([]string, error) {
	brokers := r.ListBrokers()
	dateTime := time.Unix(request.LastWritten, 0)
	topicMap := make(map[string]int)
	for id := 1; id <= len(brokers); id++ {
		fmt.Printf("Sshing into broker - %v\n", brokers[id])
		cdCmd := fmt.Sprintf("cd %s", request.DataDir)
		findTopicsCmd := fmt.Sprintf("%s | %s | %s | %s | %s", fmt.Sprintf(FindLastWrittenDirectories, request.DataDir, dateTime.UTC().Format(time.UnixDate)), RemovePathPrefix, RemovePartitionSuffix, SortAndCount, Reorder)
		data, err := r.sshCli.DialAndExecute(strings.Split(brokers[id], ":")[0], cdCmd, findTopicsCmd)

		topics := strings.Split(data.String(), "\n")
		fmt.Printf("Fetching the stale topics")
		err = r.mapTopics(topicMap, topics)
		if err != nil {
			fmt.Printf("Error while reading topics in broker %v - %v\n", id, err)
			return nil, err
		}
	}

	topics, e := r.getFullyStaleTopics(topicMap)
	return topics, e
}

func (r *KafkaRemoteClient) mapTopics(topicMap map[string]int, topics []string) error {
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

func (r *KafkaRemoteClient) getFullyStaleTopics(topicMap map[string]int) ([]string, error) {
	var staleTopics []string
	topicDetails, err := r.ListTopicDetails()
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
