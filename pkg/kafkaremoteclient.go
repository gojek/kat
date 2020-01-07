package pkg

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type sshCli interface {
	DialAndExecute(address string, commands ...shellCmd) (*bytes.Buffer, error)
}

type kafkaRemoteClient struct {
	KafkaApiClient
	sshCli
}

func NewKafkaRemoteClient(apiClient KafkaApiClient, sshClient sshCli) (KafkaSshClient, error) {
	return &kafkaRemoteClient{apiClient, sshClient}, nil
}

func (r *kafkaRemoteClient) ListTopics(request ListTopicsRequest) ([]string, error) {
	brokers := r.ListBrokers()
	topicMap := make(map[string]int)
	for id := 1; id <= len(brokers); id++ {
		fmt.Printf("Sshing into broker - %v\n", brokers[id])
		data, err := r.sshCli.DialAndExecute(strings.Split(brokers[id], ":")[0], NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir))
		if err != nil {
			fmt.Printf("Error while executing command on broker - %v\n", err)
			return nil, err
		}
		fmt.Println("Fetching the stale topics")
		topics := strings.Split(data.String(), "\n")
		err = r.mapTopics(topicMap, topics)
		if err != nil {
			fmt.Printf("Error while reading topics in broker %v - %v\n", id, err)
			return nil, err
		}
	}

	topics, e := r.getFullyStaleTopics(topicMap)
	return topics, e
}

func (r *kafkaRemoteClient) mapTopics(topicMap map[string]int, topics []string) error {
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

func (r *kafkaRemoteClient) getFullyStaleTopics(topicMap map[string]int) ([]string, error) {
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
