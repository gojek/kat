package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"syscall"
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
	Remove(fileName string) error
	Read(fileName string) ([]byte, error)
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
			topicsToMoveJSONFile:           "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile:           "/tmp/reassignment-%d.json",
			rollbackJSONFile:               "/tmp/rollback-%d.json",
			partitionsReassignmentJSONFile: "/tmp/reassign-%d/partitions-resassignment-%d.json",
		},
	}
}

type kafkaPartitionReassignment struct {
	topicsToMoveJSONFile           string
	partitionsReassignmentJSONFile string
	reassignmentJSONFile           string
	rollbackJSONFile               string
}

const kafkaReassignPartitions = "kafka-reassign-partitions.sh"
const ReassignJobResumptionFile = "/tmp/resume_reassign_job"

func (k *kafkaPartitionReassignment) generate(zookeeper, brokerList string, batchID int) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--broker-list", brokerList,
		"--topics-to-move-json-file", fmt.Sprintf(k.topicsToMoveJSONFile, batchID), "--generate"}
}

func (k *kafkaPartitionReassignment) execute(zookeeper, reassignmentJSONFile string, throttle int) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--reassignment-json-file",
		reassignmentJSONFile, "--throttle", strconv.FormatInt(int64(throttle), 10), "--execute"}
}

func (k *kafkaPartitionReassignment) verify(zookeeper, reassignmentJSONFile string) (cmd string, args []string) {
	return kafkaReassignPartitions, []string{"--zookeeper", zookeeper, "--reassignment-json-file",
		reassignmentJSONFile, "--verify"}
}

func (p *Partition) ReassignPartitions(topics []string, brokerList string, batch, timeoutPerBatchInS, pollIntervalInS, throttle, partitionBatchSize int) error {
	var batches [][]string

	for i := 0; i < len(topics); i += batch {
		batches = append(batches, topics[i:min(i+batch, len(topics))])
	}

	baseCtx, cancelContextFunc := context.WithCancel(context.Background())

	sigTermHandler := io.SignalHandler{}
	sigTermHandler.SetListener(baseCtx, cancelContextFunc, syscall.SIGINT)
	logger.Info("Set up SIGTERM listener")
	defer sigTermHandler.Close()

	defer cancelContextFunc()

	for id, batch := range batches {
		if err := p.executeReassignment(batch, id, throttle, pollIntervalInS, timeoutPerBatchInS, partitionBatchSize, brokerList); err != nil {
			return err
		}

		if err := p.Write(ReassignJobResumptionFile, batch[len(batch)-1]); err != nil {
			return err
		}

		select {
		case <-baseCtx.Done():
			return fmt.Errorf("stopping due to interrupt, migration of %s was completed", batch[len(batch)-1])
		case <-time.After(time.Millisecond * 500):
		}
	}

	err := p.Remove(ReassignJobResumptionFile)
	if err != nil {
		logger.Errorf("Error while trying to cleanup job resumption file, %s", err)
	}
	return nil
}

func (p *Partition) executeReassignment(batch []string, topicBatchID, throttle, pollIntervalInS, timeoutPerBatchInS,
	partitionBatchSize int, brokerList string) error {
	err := p.createTopicsToMoveJSON(batch, topicBatchID)
	if err != nil {
		return err
	}

	err = p.generateReassignmentAndRollbackJSON(brokerList, topicBatchID)
	if err != nil {
		return err
	}

	partitionBatchCount, err := p.createPartitionBatchedReassignmentFiles(topicBatchID, partitionBatchSize)
	if err != nil {
		return err
	}

	for partitionBatchID := 0; partitionBatchID < partitionBatchCount; partitionBatchID++ {
		reassignmentJSONFile := fmt.Sprintf(p.kafkaPartitionReassignment.partitionsReassignmentJSONFile, topicBatchID, partitionBatchID)
		_, err = p.Execute(p.execute(p.zookeeper, reassignmentJSONFile, throttle))
		if err != nil {
			return err
		}

		err = p.pollStatus(pollIntervalInS, timeoutPerBatchInS, reassignmentJSONFile)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Partition) createPartitionBatchedReassignmentFiles(topicBatchID, partitionBatchSize int) (int, error) {
	f, err := p.Read(fmt.Sprintf(p.kafkaPartitionReassignment.reassignmentJSONFile, topicBatchID))
	if err != nil {
		return 0, fmt.Errorf("error while reading topic batched reassignment file %s", err)
	}

	var reassignmentObject reassignmentJSON
	err = json.Unmarshal(f, &reassignmentObject)
	if err != nil {
		return 0, fmt.Errorf("error while unmarshaling topic batched reassignment json %s", err)
	}
	if len(reassignmentObject.Partitions) != 0 {
		partitionBatchCount := 0
		totalPartitions := len(reassignmentObject.Partitions)
		logger.Infof("%d partitions to be moved in batch id: %d", totalPartitions, topicBatchID)
		for i := 0; i < totalPartitions; i += partitionBatchSize {
			partitionBatchedReassignment := reassignmentJSON{Version: 1,
				Partitions: reassignmentObject.Partitions[i:min(i+partitionBatchSize, totalPartitions)]}
			dataToWrite, err := json.Marshal(partitionBatchedReassignment)
			if err != nil {
				return 0, fmt.Errorf("error while marshaling partition batched reassignment data %s", err)
			}
			err = p.Write(fmt.Sprintf(p.partitionsReassignmentJSONFile, topicBatchID, partitionBatchCount), string(dataToWrite))
			if err != nil {
				return 0, fmt.Errorf("error while writing partition based reassignment batches. %s", err)
			}
			partitionBatchCount++
		}
		logger.Infof("%d partition batches were created", partitionBatchCount)
		return partitionBatchCount, nil
	}
	return 0, errors.New("no partitions to reassign in partition reassignment file")
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

		err = p.pollStatus(pollIntervalInS, timeoutPerBatchInS, fmt.Sprintf(p.kafkaPartitionReassignment.reassignmentJSONFile, id))
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

func (p *Partition) verifyAssignmentCompletion(fileName string) error {
	verificationData, err := p.Execute(p.verify(p.zookeeper, fileName))
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

func (p *Partition) pollStatus(pollIntervalInS, timeoutInS int, reassignFileToPoll string) error {
	logger.Infof("Polling partition reassignment status until %v seconds\n", timeoutInS)
	num := math.Ceil(float64(timeoutInS) / float64(pollIntervalInS))
	var err error

	for i := 0; i < int(num); i++ {
		logger.Info("Verifying Partitioner Reassignment ...")
		err = p.verifyAssignmentCompletion(reassignFileToPoll)
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

	reassignmentData, err := p.Execute(p.execute(p.zookeeper, fmt.Sprintf(p.reassignmentJSONFile, batchID), throttle))
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
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Replicas  []int32  `json:"replicas"`
	LogDirs   []string `json:"log_dirs"`
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
