package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/gojek/kat/logger"
	"github.com/gojek/kat/pkg/client"
	"github.com/gojek/kat/pkg/io"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func init() {
	logger.SetupLogger("info")
}

func TestPartition_ReassignPartitions_CreateTopicsToMoveFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper:                  "zoo",
		executor:                   executor,
		file:                       file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{},
	}
	topics := []string{"test-1", "test-2"}
	expectedErr := errors.New("error")
	file.On("Write", mock.Anything, mock.Anything).Return(expectedErr)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 10, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_CreateTopicsSuccess_GenerateReassignmentFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper:                  "zoo",
		executor:                   executor,
		file:                       file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{},
	}
	topics := []string{"test-1", "test-2"}
	expectedTopicsToMove := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}, {"topic": "test-2"}}}
	expectedErr := errors.New("error")
	file.On("Write", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		expectedJSON, _ := json.MarshalIndent(expectedTopicsToMove, "", "")
		assert.Equal(t, string(expectedJSON), args[1])
	}).Return(nil)
	executor.On("Execute", mock.Anything, mock.Anything).Return(bytes.Buffer{}, expectedErr)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 10, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_GenerateReassignmentAndRollbackSuccess_ExecuteFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}

	expectedTopicsToMove := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}, {"topic": "test-2"}}}
	expectedTopicsJSON, _ := json.MarshalIndent(expectedTopicsToMove, "", "")
	expectedErr := errors.New("error")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON)).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes, nil)

	expectedRollbackJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, expectedErr)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 10, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_ExecuteSuccess_PollFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}

	expectedTopicsToMove := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}, {"topic": "test-2"}}}
	expectedTopicsJSON, _ := json.MarshalIndent(expectedTopicsToMove, "", "")
	expectedErr := errors.New("Partitioner Reassignment failed: Reassignment of partition test-1-0 failed")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON)).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes, nil)

	expectedRollbackJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 failed\n" +
		"Reassignment of partition test-2-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 1, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_Success(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}

	expectedTopicsToMove := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}, {"topic": "test-2"}}}
	expectedTopicsJSON, _ := json.MarshalIndent(expectedTopicsToMove, "", "")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON)).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes, nil)

	expectedRollbackJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON).Return(nil)
	file.On("Write", ReassignJobResumptionFile, "test-2").Return(nil)
	file.On("Remove", ReassignJobResumptionFile).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 completed successfully\n" +
		"Reassignment of partition test-2-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 1, 1, 100000)
	assert.NoError(t, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_GracefulPause(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}

	expectedTopicsToMove1 := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}}}
	expectedTopicsJSON1, _ := json.MarshalIndent(expectedTopicsToMove1, "", "")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON1)).Return(nil)

	expectedFullReassignmentBytes1 := bytes.Buffer{}
	expectedFullReassignmentBytes1.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes1, nil)

	expectedRollbackJSON1 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON1 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON1).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON1).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)

	expectedVerificationBytes1 := bytes.Buffer{}
	expectedVerificationBytes1.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes1, nil)

	file.On("Write", ReassignJobResumptionFile, "test-1").Return(nil).Times(1)

	pid := syscall.Getpid()
	time.AfterFunc(300*time.Millisecond, func() {
		syscall.Kill(pid, syscall.SIGINT)
	})

	err := partition.ReassignPartitions(topics, "broker-list", 1, 1, 1, 100000)
	assert.Error(t, err)
	assert.EqualError(t, err, "stopping due to interrupt, migration of test-1 was completed")
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_PollUntilTimeoutIfNotYetSuccessful(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}
	expectedErr := errors.New("Partitioner Reassignment failed: Reassignment of partition test-1-0 is inprogress")

	expectedTopicsToMove := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}, {"topic": "test-2"}}}
	expectedTopicsJSON, _ := json.MarshalIndent(expectedTopicsToMove, "", "")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON)).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes, nil)

	expectedRollbackJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 is inprogress\n" +
		"Reassignment of partition test-2-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil).Times(3)

	err := partition.ReassignPartitions(topics, "broker-list", 2, 3, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_ReassignPartitions_Success_ForMultipleBatches(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topics := []string{"test-1", "test-2"}

	expectedTopicsToMove1 := topicsToMove{Topics: []map[string]string{{"topic": "test-1"}}}
	expectedTopicsJSON1, _ := json.MarshalIndent(expectedTopicsToMove1, "", "")
	file.On("Write", "/tmp/topics-to-move-0.json", string(expectedTopicsJSON1)).Return(nil)

	expectedTopicsToMove2 := topicsToMove{Topics: []map[string]string{{"topic": "test-2"}}}
	expectedTopicsJSON2, _ := json.MarshalIndent(expectedTopicsToMove2, "", "")
	file.On("Write", "/tmp/topics-to-move-1.json", string(expectedTopicsJSON2)).Return(nil)

	expectedFullReassignmentBytes1 := bytes.Buffer{}
	expectedFullReassignmentBytes1.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-0.json", "--generate"}).Return(expectedFullReassignmentBytes1, nil)

	expectedRollbackJSON1 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON1 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[1,2,3],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-0.json", expectedRollbackJSON1).Return(nil)
	file.On("Write", "/tmp/reassignment-0.json", expectedReassignmentJSON1).Return(nil)

	expectedFullReassignmentBytes2 := bytes.Buffer{}
	expectedFullReassignmentBytes2.WriteString("Current partition replica assignment\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n" +
		"                       \n" +
		"Proposed partition reassignment configuration\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--broker-list", "broker-list", "--topics-to-move-json-file", "/tmp/topics-to-move-1.json", "--generate"}).Return(expectedFullReassignmentBytes2, nil)

	expectedRollbackJSON2 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	expectedReassignmentJSON2 := "{\"version\":1,\"partitions\":[{\"topic\":\"test-2\",\"partition\":0,\"replicas\":[3,5,6],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}"
	file.On("Write", "/tmp/rollback-1.json", expectedRollbackJSON2).Return(nil)
	file.On("Write", "/tmp/reassignment-1.json", expectedReassignmentJSON2).Return(nil)

	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-1.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, nil)

	expectedVerificationBytes1 := bytes.Buffer{}
	expectedVerificationBytes1.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes1, nil)

	expectedVerificationBytes2 := bytes.Buffer{}
	expectedVerificationBytes2.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-2-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-1.json", "--verify"}).Return(expectedVerificationBytes2, nil)

	file.On("Write", ReassignJobResumptionFile, "test-1").Return(nil)
	file.On("Write", ReassignJobResumptionFile, "test-2").Return(nil)
	file.On("Remove", ReassignJobResumptionFile).Return(nil)

	err := partition.ReassignPartitions(topics, "broker-list", 1, 1, 1, 100000)
	assert.NoError(t, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplication_WriteReassignmentFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	expectedErr := errors.New("error")

	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(expectedErr)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 3, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplication_WriteReassignmentSuccess_ExecuteFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	expectedErr := errors.New("error")

	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(nil)
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(bytes.Buffer{}, expectedErr)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 3, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplication_ExecuteSuccess_RollbackJSONFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	expectedErr := errors.New("error")

	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(nil)
	file.On("Write", "/tmp/rollback-0.json", mock.Anything).Return(expectedErr)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" + "\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(expectedFullReassignmentBytes, nil)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 3, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplication_RollbackJSONSuccess_PollFailure(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	expectedErr := errors.New("Partitioner Reassignment failed: Reassignment of partition test-1-0 failed")

	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(nil)
	file.On("Write", "/tmp/rollback-0.json", mock.Anything).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" + "\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(expectedFullReassignmentBytes, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 failed\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 1, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplicationSuccess(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(nil)
	file.On("Write", "/tmp/rollback-0.json", mock.Anything).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" + "\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(expectedFullReassignmentBytes, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 completed successfully\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 1, 1, 100000)
	assert.NoError(t, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestPartition_IncreaseReplication__PollUntilTimeoutIfNotYetSuccessful(t *testing.T) {
	executor := &io.MockExecutor{}
	file := &MockFile{}
	partition := &Partition{
		zookeeper: "zoo",
		executor:  executor,
		file:      file,
		kafkaPartitionReassignment: kafkaPartitionReassignment{
			topicsToMoveJSONFile: "/tmp/topics-to-move-%d.json",
			reassignmentJSONFile: "/tmp/reassignment-%d.json",
			rollbackJSONFile:     "/tmp/rollback-%d.json",
		},
	}
	expectedErr := errors.New("Partitioner Reassignment failed: Reassignment of partition test-1-0 is inprogress")
	topicsMetadata := []*client.TopicMetadata{{
		Err:        nil,
		Name:       "test-1",
		IsInternal: false,
		Partitions: []*client.PartitionMetadata{{
			Err:             nil,
			ID:              1,
			Leader:          1,
			Replicas:        []int32{1},
			Isr:             []int32{1},
			OfflineReplicas: nil,
		}},
	}}
	file.On("Write", "/tmp/reassignment-0.json", mock.Anything).Return(nil)
	file.On("Write", "/tmp/rollback-0.json", mock.Anything).Return(nil)

	expectedFullReassignmentBytes := bytes.Buffer{}
	expectedFullReassignmentBytes.WriteString("Current partition replica assignment\n" + "\n" +
		"{\"version\":1,\"partitions\":[{\"topic\":\"test-1\",\"partition\":0,\"replicas\":[6,1,2],\"log_dirs\":[\"any\",\"any\",\"any\"]}, {\"topic\":\"test-2\",\"partition\":0,\"replicas\":[4,2,5],\"log_dirs\":[\"any\",\"any\",\"any\"]}]}\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--throttle", "100000", "--execute"}).Return(expectedFullReassignmentBytes, nil)

	expectedVerificationBytes := bytes.Buffer{}
	expectedVerificationBytes.WriteString("Status of partition reassignment: \n" +
		"Reassignment of partition test-1-0 is inprogress\n")
	executor.On("Execute", "kafka-reassign-partitions.sh", []string{"--zookeeper", "zoo", "--reassignment-json-file", "/tmp/reassignment-0.json", "--verify"}).Return(expectedVerificationBytes, nil).Times(3)

	err := partition.IncreaseReplication(topicsMetadata, 1, 1, 1, 3, 1, 100000)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	executor.AssertExpectations(t)
	file.AssertExpectations(t)
}

func TestBuildReassignmentJSON(suite *testing.T) {
	suite.Run("Build Reassignment JSON", func(t *testing.T) {
		partitionMetadata1 := client.PartitionMetadata{ID: 8, Leader: 6, Replicas: []int32{6}}
		partitionMetadata2 := client.PartitionMetadata{ID: 11, Leader: 3, Replicas: []int32{3}}
		partitionMetadata3 := client.PartitionMetadata{ID: 2, Leader: 6, Replicas: []int32{6}}
		partitionMetadata4 := client.PartitionMetadata{ID: 5, Leader: 3, Replicas: []int32{3}}
		partitionMetadata5 := client.PartitionMetadata{ID: 4, Leader: 2, Replicas: []int32{2}}
		partitionMetadata6 := client.PartitionMetadata{ID: 7, Leader: 5, Replicas: []int32{5}}
		partitionMetadata7 := client.PartitionMetadata{ID: 10, Leader: 2, Replicas: []int32{2}}
		partitionMetadata8 := client.PartitionMetadata{ID: 1, Leader: 5, Replicas: []int32{5}}
		partitionMetadata9 := client.PartitionMetadata{ID: 9, Leader: 1, Replicas: []int32{1}}
		partitionMetadata10 := client.PartitionMetadata{ID: 3, Leader: 1, Replicas: []int32{1}}
		partitionMetadata11 := client.PartitionMetadata{ID: 6, Leader: 4, Replicas: []int32{4}}
		partitionMetadata12 := client.PartitionMetadata{ID: 0, Leader: 4, Replicas: []int32{4}}
		topicMetadata := client.TopicMetadata{Name: "topic", Partitions: []*client.PartitionMetadata{&partitionMetadata1, &partitionMetadata2, &partitionMetadata3, &partitionMetadata4, &partitionMetadata5, &partitionMetadata6, &partitionMetadata7, &partitionMetadata8, &partitionMetadata9, &partitionMetadata10, &partitionMetadata11, &partitionMetadata12}}
		expectedJSONForReplicationFactor3 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 3, 4}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 6, 1}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 5, 6}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 2, 3}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 4, 5}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 1, 2}}}}
		expectedJSONForReplicationFactor4 := reassignmentJSON{Version: 1, Partitions: []partitionDetail{{Topic: "topic", Partition: 8, Replicas: []int32{6, 1, 2, 3}}, {Topic: "topic", Partition: 11, Replicas: []int32{3, 4, 5, 6}}, {Topic: "topic", Partition: 2, Replicas: []int32{6, 4, 5, 1}}, {Topic: "topic", Partition: 5, Replicas: []int32{3, 1, 2, 4}}, {Topic: "topic", Partition: 4, Replicas: []int32{2, 3, 4, 5}}, {Topic: "topic", Partition: 7, Replicas: []int32{5, 6, 1, 2}}, {Topic: "topic", Partition: 10, Replicas: []int32{2, 6, 1, 3}}, {Topic: "topic", Partition: 1, Replicas: []int32{5, 3, 4, 6}}, {Topic: "topic", Partition: 9, Replicas: []int32{1, 2, 3, 4}}, {Topic: "topic", Partition: 3, Replicas: []int32{1, 5, 6, 2}}, {Topic: "topic", Partition: 6, Replicas: []int32{4, 5, 6, 1}}, {Topic: "topic", Partition: 0, Replicas: []int32{4, 2, 3, 5}}}}

		actualJSONForReplicationFactor3 := buildReassignmentJSON([]*client.TopicMetadata{&topicMetadata}, 3, 6)
		actualJSONForReplicationFactor4 := buildReassignmentJSON([]*client.TopicMetadata{&topicMetadata}, 4, 6)

		assert.Equal(t, expectedJSONForReplicationFactor3, actualJSONForReplicationFactor3)
		assert.Equal(t, expectedJSONForReplicationFactor4, actualJSONForReplicationFactor4)
	})
}

type MockFile struct {
	mock.Mock
}

func (m *MockFile) Write(fileName, data string) error {
	arguments := m.Called(fileName, data)
	return arguments.Error(0)
}

func (m *MockFile) Remove(fileName string) error {
	arguments := m.Called(fileName)
	return arguments.Error(0)
}
