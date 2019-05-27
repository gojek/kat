package topic

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"math/rand"
	"source.golabs.io/hermes/kafka-admin-tools/utils"
)

var increaseReplicationFactorCmd = &cobra.Command{
	Use:   "increase-replication-factor",
	Short: "Increases the replication factor for the given topics by the given number",
	Run:   increaseReplicationFactor,
}

func init() {
	increaseReplicationFactorCmd.PersistentFlags().StringP("topics", "t", "", "Comma separated list of topic names to describe")
	increaseReplicationFactorCmd.PersistentFlags().IntP("replication-factor", "r", 0, "New Replication Factor")
	increaseReplicationFactorCmd.PersistentFlags().IntP("num-of-brokers", "n", 0, "Number of brokers in the cluster")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("topics")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("replication-factor")
	increaseReplicationFactorCmd.MarkPersistentFlagRequired("num-of-brokers")
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

	metadata, err := admin.DescribeTopics(topics)
	if err != nil {
		fmt.Printf("Error while fetching topic metadata: %v\n", err)
		return
	}

	reassignmentJson := buildReassignmentJson(metadata, replicationFactor, numOfBrokers)
	bytes, err := json.Marshal(reassignmentJson)
	fmt.Println(string(bytes))
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