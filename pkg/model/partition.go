package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/gojek/kat/pkg/client"
	"github.com/gojek/kat/pkg/io"

	"github.com/gojek/kat/logger"
)

type executor interface {
	Execute(name string, args []string) (bytes.Buffer, error)
}

type file interface {
	Write(fileName, data string) error
}

type Partition struct {
	zookeeper string
	executor
	file
	kafkaPartitionReassignment
}

func NewPartition(zookeeper string) *Partition {
	return &Partition{
		zookeeper: zookeeper,
		executor:  &io.Executor{},
		file:      &io.File{},
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
}

type kafkaPartitionReassignment struct {
	topicsToMoveJSONFile string
	reassignmentJSONFile string
	rollbackJSONFile     string
}

const kafkaReassignPartitions = "kafka-reassign-partitions"
const REASSIGN_JOB_RESUMPTION_FILE = "/tmp/reassign_job_state"

func (k *kafkaPartitionReassignment) generate(zookeeper, brokerList string, batchID int) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--broker-list", brokerList,
		"--topics-to-move-json-file", fmt.Sprintf(k.topicsToMoveJSONFile, batchID), "--generate"}
}

func (k *kafkaPartitionReassignment) execute(zookeeper string, batchID, throttle int) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--reassignment-json-file",
		fmt.Sprintf(k.reassignmentJSONFile, batchID), "--throttle", strconv.FormatInt(int64(throttle), 10), "--execute"}
}

func (k *kafkaPartitionReassignment) verify(zookeeper string, batchID int) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--reassignment-json-file",
		fmt.Sprintf(k.reassignmentJSONFile, batchID), "--verify"}
}

func (p *Partition) ReassignPartitions(topics []string, brokerList string, batch, timeoutPerBatchInS, pollIntervalInS, throttle int) error {
	var batches [][]string

	for i := 0; i < len(topics); i += batch {
		batches = append(batches, topics[i:min(i+batch, len(topics))])
	}

	for id, batch := range batches {
		err := p.createTopicsToMoveJSON(batch, id)
		if err != nil {
			return err
		}

		err = p.generateReassignmentAndRollbackJSON(brokerList, id)
		if err != nil {
			return err
		}

		_, err = p.Execute(p.execute(p.zookeeper, id, throttle))
		if err != nil {
			return err
		}

		err = p.pollStatus(pollIntervalInS, timeoutPerBatchInS, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Partition) IncreaseReplication(topicsMetadata []*client.TopicMetadata, replicationFactor, numOfBrokers,
	batch, timeoutPerBatchInS, pollIntervalInS, throttle int) error {
	var batches [][]*client.TopicMetadata

	for i := 0; i < len(topicsMetadata); i += batch {
		batches = append(batches, topicsMetadata[i:min(i+batch, len(topicsMetadata))])
	}

	for id, batch := range batches {
		err := p.reassignForBatch(batch, id, replicationFactor, numOfBrokers, throttle)
		if err != nil {
			return err
		}

		err = p.pollStatus(pollIntervalInS, timeoutPerBatchInS, id)
		if err != nil {
			return err
		}
	}
	return nil
}

type topicsToMove struct {
	Topics []map[string]string `json:"topics"`
}

func (t *topicsToMove) add(topic string) {
	t.Topics = append(t.Topics, map[string]string{"topic": topic})
}

func (p *Partition) createTopicsToMoveJSON(batch []string, batchID int) error {
	topicsToMoveStruct := topicsToMove{}
	for _, topic := range batch {
		topicsToMoveStruct.add(topic)
	}
	topicsData, err := json.MarshalIndent(topicsToMoveStruct, "", "")
	if err != nil {
		return err
	}
	err = p.Write(fmt.Sprintf(p.topicsToMoveJSONFile, batchID), string(topicsData))

	return err
}

func (p *Partition) generateReassignmentAndRollbackJSON(brokerList string, batchID int) error {
	reassignmentData, err := p.Execute(p.generate(p.zookeeper, brokerList, batchID))
	if err != nil {
		return err
	}

	fullReassignmentOutput := strings.Split(reassignmentData.String(), "\n")
	err = p.Write(fmt.Sprintf(p.rollbackJSONFile, batchID), fullReassignmentOutput[1])
	if err != nil {
		return err
	}

	err = p.Write(fmt.Sprintf(p.reassignmentJSONFile, batchID), fullReassignmentOutput[4])

	return err
}

func (p *Partition) verifyAssignmentCompletion(batchID int) error {
	verificationData, err := p.Execute(p.verify(p.zookeeper, batchID))
	if err != nil {
		return err
	}
	verificationOutput := strings.Split(verificationData.String(), "\n")
	var errorArray []string
	for _, result := range verificationOutput {
		logger.Info(result)
		if strings.Contains(result, "Status") || strings.Contains(result, "Throttle was removed.") {
			continue
		}
		if result != "" && !strings.Contains(result, "successfully") {
			errorArray = append(errorArray, fmt.Sprintf("Partitioner Reassignment failed: %s", result))
		}
	}
	if len(errorArray) != 0 {
		return errors.New(strings.Join(errorArray, ","))
	}

	return nil
}

func (p *Partition) pollStatus(pollIntervalInS, timeoutInS, batchID int) error {
	logger.Infof("Polling partition reassignment status until %v seconds\n", timeoutInS)
	num := math.Ceil(float64(timeoutInS) / float64(pollIntervalInS))
	var err error

	for i := 0; i < int(num); i++ {
		logger.Info("Verifying Partitioner Reassignment ...")
		err = p.verifyAssignmentCompletion(batchID)
		if err == nil {
			break
		}
		fmt.Println("---------------------------------------------------------")
		time.Sleep(time.Duration(pollIntervalInS) * time.Second)
	}

	return err
}

func (p *Partition) reassignForBatch(batch []*client.TopicMetadata, batchID, replicationFactor, numOfBrokers, throttle int) error {
	data, err := json.MarshalIndent(buildReassignmentJSON(batch, replicationFactor, numOfBrokers), "", "")
	if err != nil {
		return err
	}
	err = p.Write(fmt.Sprintf(p.reassignmentJSONFile, batchID), string(data))
	if err != nil {
		return err
	}

	reassignmentData, err := p.Execute(p.execute(p.zookeeper, batchID, throttle))
	if err != nil {
		return err
	}
	fullReassignmentOutput := strings.Split(reassignmentData.String(), "\n")
	err = p.Write(fmt.Sprintf(p.rollbackJSONFile, batchID), fullReassignmentOutput[2])
	if err != nil {
		return err
	}

	logger.Info(reassignmentData.String())
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

func buildReassignmentJSON(batch []*client.TopicMetadata, replicationFactor, numOfBrokers int) reassignmentJSON {
	reassignmentData := reassignmentJSON{Version: 1, Partitions: []partitionDetail{}}
	for _, topicMetadata := range batch {
		partitions := topicMetadata.Partitions
		leaderCounter := make(map[int32]int32)
		for _, partitionMetadata := range partitions {
			replicas := buildReplicaSet(partitionMetadata.Leader, int32(replicationFactor), int32(numOfBrokers), leaderCounter)
			partitionData := partitionDetail{Topic: topicMetadata.Name, Partition: partitionMetadata.ID, Replicas: replicas}
			reassignmentData.Partitions = append(reassignmentData.Partitions, partitionData)
		}
	}
	return reassignmentData
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
	leaderCounter[leader]++
	return replicas
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
