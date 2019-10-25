package pkg

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os/exec"
)

type Topic struct {
	client KafkaClient
}

type TopicCli interface {
	List() (map[string]TopicDetail, error)
	Describe(topics []string) ([]*TopicMetadata, error)
	ShowConfig(topic string) ([]ConfigEntry, error)
	UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error
	IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) error
}

func NewTopic(client KafkaClient) *Topic {
	return &Topic{client: client}
}

func (t *Topic) List() (map[string]TopicDetail, error) {
	return t.client.ListTopicDetails()
}

func (t *Topic) Describe(topics []string) ([]*TopicMetadata, error) {
	return t.client.DescribeTopicMetadata(topics)
}

func (t *Topic) UpdateConfig(topics []string, configMap map[string]*string, validateOnly bool) error {
	for _, topicName := range topics {
		err := t.client.UpdateConfig(t.client.GetTopicResourceType(), topicName, configMap, validateOnly)
		if err != nil {
			fmt.Printf("Err while updating config for topic - %v: %v\n", topicName, err)
			return err
		}
		fmt.Printf("Config was successfully updated for topic - %v\n", topicName)
	}
	return nil
}

func (t *Topic) ShowConfig(topic string) ([]ConfigEntry, error) {
	configResource := ConfigResource{Name: topic, Type: t.client.GetTopicResourceType()}
	return t.client.ShowConfig(configResource)
}

func (t *Topic) IncreaseReplicationFactor(topics []string, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) error {
	metadata, err := t.Describe(topics)
	if err != nil {
		fmt.Printf("Error while fetching topic metadata: %v\n", err)
		return err
	}

	for _, topicMetadata := range metadata {
		fmt.Printf("Increasing replication factor for topic: %v\n", (*topicMetadata).Name)
		err = reassignForTopic(*topicMetadata, replicationFactor, numOfBrokers, kafkaPath, zookeeper)
		if err != nil {
			fmt.Printf("Failed to increase replication factor for topic: %v\n", (*topicMetadata).Name)
			return err
		}
		fmt.Printf("Successfully increased replication factor for topic: %v\n", (*topicMetadata).Name)
	}

	return nil
}

func reassignForTopic(topicMetadata TopicMetadata, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) error {
	data, err := json.MarshalIndent(buildReassignmentJSON(topicMetadata, replicationFactor, numOfBrokers), "", "")
	err = ioutil.WriteFile("/tmp/increase-replication-factor.json", data, 0644)
	if err != nil {
		fmt.Printf("Error while creating increase-replication-factor.json: %v\n", err)
		return err
	}

	command := exec.Command("cd", kafkaPath)
	command = exec.Command("kafka-reassign-partitions", "--zookeeper", zookeeper, "--reassignment-json-file", "/tmp/increase-replication-factor.json", "--execute")
	var outb, errb bytes.Buffer
	command.Stdout = &outb
	command.Stderr = &errb
	err = command.Run()
	if err != nil {
		fmt.Printf("Error while increasing replication factor: %v\n", err)
		fmt.Println(errb.String())
		return err
	}
	fmt.Println(outb.String())
	return nil
}

type partitionDetail struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
}

type reassignmentJSON struct {
	Version    int               `json:"version"`
	Partitions []partitionDetail `json:"partitions"`
}

func buildReassignmentJSON(topicMetadata TopicMetadata, replicationFactor, numOfBrokers int) reassignmentJSON {
	reassignmentJSON := reassignmentJSON{Version: 1, Partitions: []partitionDetail{}}
	partitions := topicMetadata.Partitions
	leaderCounter := make(map[int32]int32)
	for _, partitionMetadata := range partitions {
		replicas := buildReplicaSet((*partitionMetadata).Leader, int32(replicationFactor), int32(numOfBrokers), leaderCounter)
		partitionDetail := partitionDetail{Topic: topicMetadata.Name, Partition: (*partitionMetadata).ID, Replicas: replicas}
		reassignmentJSON.Partitions = append(reassignmentJSON.Partitions, partitionDetail)
	}
	return reassignmentJSON
}

func buildReplicaSet(leader, replicationFactor, numOfBrokers int32, leaderCounter map[int32]int32) []int32 {
	replicas := []int32{leader}
	skipFactor := 0
	for i := 1; i < int(replicationFactor); {
		replica := (leader + ((replicationFactor-1)*leaderCounter[leader] + int32(i)) + int32(skipFactor)) % numOfBrokers
		if replica == 0 {
			replica = numOfBrokers
		}
		if replica == leader {
			skipFactor = 1
			continue
		}
		replicas = append(replicas, replica)
		i++
	}
	leaderCounter[leader] = leaderCounter[leader] + 1
	return replicas
}
