package topic

import (
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gojekfarm/kat/util"
	"github.com/spf13/cobra"
	"io/ioutil"
	"os/exec"
)

var increaseReplicationFactorCmd = &cobra.Command{
	Use:   "increase-replication-factor",
	Short: "Increases the replication factor for the given topics by the given number",
	Run: func(cmd *cobra.Command, args []string) {
		u := util.NewCobraUtil(cmd)
		admin := u.GetAdminClient("broker-list")
		topics := u.GetTopicNames()
		replicationFactor := u.GetIntArg("replication-factor")
		numOfBrokers := u.GetIntArg("num-of-brokers")
		kafkaPath := u.GetCmdArg("kafka-path")
		zookeeper := u.GetCmdArg("zookeeper")
		increaseReplicationFactor(admin, topics, replicationFactor, numOfBrokers, kafkaPath, zookeeper)
	},
}

func init() {
	increaseReplicationFactorCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	increaseReplicationFactorCmd.PersistentFlags().StringP("zookeeper", "z", "", "Comma separated list of zookeeper ips")
	increaseReplicationFactorCmd.PersistentFlags().IntP("replication-factor", "r", 0, "New Replication Factor")
	increaseReplicationFactorCmd.PersistentFlags().IntP("num-of-brokers", "n", 0, "Number of brokers in the cluster")
	increaseReplicationFactorCmd.PersistentFlags().StringP("kafka-path", "p", "", "Path to the kafka executable")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("topics")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("zookeeper")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("replication-factor")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("num-of-brokers")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("kafka-path")
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

func increaseReplicationFactor(admin sarama.ClusterAdmin, topics []string, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) {
	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Error while fetching topic metadata: %v\n", err)
		return
	}

	for _, topicMetadata := range metadata {
		fmt.Printf("Increasing replication factor for topic: %v\n", (*topicMetadata).Name)
		err = reassignForTopic(*topicMetadata, replicationFactor, numOfBrokers, kafkaPath, zookeeper)
		if err != nil {
			fmt.Printf("Failed to increase replication factor for topic: %v\n", (*topicMetadata).Name)
		} else {
			fmt.Printf("Successfully increased replication factor for topic: %v\n", (*topicMetadata).Name)
		}
	}
}

func reassignForTopic(topicMetadata sarama.TopicMetadata, replicationFactor, numOfBrokers int, kafkaPath, zookeeper string) error {
	bytes, err := json.MarshalIndent(buildReassignmentJSON(topicMetadata, replicationFactor, numOfBrokers), "", "")
	err = ioutil.WriteFile("/tmp/increase-replication-factor.json", bytes, 0644)
	if err != nil {
		fmt.Printf("Error while creating increase-replication-factor.json: %v\n", err)
		return err
	}

	command := exec.Command("cd", kafkaPath)
	command = exec.Command("kafka-reassign-partitions", "--zookeeper", zookeeper, "--reassignment-json-file", "/tmp/increase-replication-factor.json", "--execute")
	var outb, errb bytes2.Buffer
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

func buildReassignmentJSON(topicMetadata sarama.TopicMetadata, replicationFactor, numOfBrokers int) reassignmentJSON {
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
