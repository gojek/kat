package topic

import (
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"io/ioutil"
	"math/rand"
	"os/exec"
	"source.golabs.io/hermes/kafka-admin-tools/utils"
)

var increaseReplicationFactorCmd = &cobra.Command{
	Use:   "increase-replication-factor",
	Short: "Increases the replication factor for the given topics by the given number",
	Run:   increaseReplicationFactor,
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

type reassignmentJson struct {
	Version    int               `json:"version"`
	Partitions []partitionDetail `json:"partitions"`
}

func increaseReplicationFactor(cmd *cobra.Command, args []string) {
	admin := utils.GetAdminClient(cmd)
	topics := getTopicNames(cmd)
	replicationFactor := getReplicationFactor(cmd)
	numOfBrokers := getNumOfBrokers(cmd)
	kafkaPath := getKafkaPath(cmd)
	zookeeper := getZookeeper(cmd)

	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Error while fetching topic metadata: %v\n", err)
		return
	}

	reassignmentJson := buildReassignmentJson(metadata, replicationFactor, numOfBrokers)
	bytes, err := json.MarshalIndent(reassignmentJson, "", "")
	err = ioutil.WriteFile("/tmp/increase-replication-factor.json", bytes, 0644)
	if err!= nil {
		fmt.Printf("Error while creating increase-replication-factor.json: %v\n", err)
		return
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
		return
	}
	fmt.Println(outb.String())
}

func contains(arr []int32, elem int32) bool {
	for _, e := range arr {
		if e == elem {
			return true
		}
	}
	return false
}

func buildReassignmentJson(metadata []*sarama.TopicMetadata, replicationFactor, numOfBrokers int) reassignmentJson {
	reassignmentJson := reassignmentJson{Version: 1, Partitions: []partitionDetail{}}
	for _, topicMetadata := range metadata {
		partitions := (*topicMetadata).Partitions
		for _, partitionMetadata := range partitions {
			replicas := []int32{(*partitionMetadata).Leader}
			for r := 1; r <= replicationFactor-1; {
				replica := int32(rand.Intn(numOfBrokers) + 1)
				if contains(replicas, replica) {
					continue
				}
				replicas = append(replicas, replica)
				r++
			}
			partitionDetail := partitionDetail{Topic: (*topicMetadata).Name, Partition: (*partitionMetadata).ID, Replicas: replicas}
			reassignmentJson.Partitions = append(reassignmentJson.Partitions, partitionDetail)
		}
	}
	return reassignmentJson
}