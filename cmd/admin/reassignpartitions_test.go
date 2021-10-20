package admin

import (
	"errors"
	"os"
	"testing"

	"github.com/gojek/kat/pkg/client"

	"bou.ke/monkey"
	"github.com/gojek/kat/logger"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetupLogger("info")
}

func TestReassignPartitions_Success(t *testing.T) {
	mockLister := &client.MockLister{}
	mockPartitioner := &client.MockPartitioner{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topics := []string{"topic-1"}
	topicRegex := "topic-1"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockPartitioner.On("ReassignPartitions", topics, brokerIds, batch, timeoutPerBatch, pollInterval, throttle).Return(nil).Times(1)
	r := reassignPartitions{Lister: mockLister, Partitioner: mockPartitioner, topics: topicRegex, brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	r.reassignPartitions()
	mockLister.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestReassignPartitions_ListFailure(t *testing.T) {
	mockLister := &client.MockLister{}
	mockPartitioner := &client.MockPartitioner{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	var topics []string
	topicRegex := "topic-1"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	r := reassignPartitions{Lister: mockLister, Partitioner: mockPartitioner, topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	assert.PanicsWithValue(t, "os.Exit called", r.reassignPartitions, "os.Exit was not called")

	mockPartitioner.AssertNotCalled(t, "ReassignPartitions", topics, brokerIds, batch, timeoutPerBatch, pollInterval, throttle)
	mockLister.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestReassignPartitions_NoMatch(t *testing.T) {
	mockLister := &client.MockLister{}
	mockPartitioner := &client.MockPartitioner{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	var topics []string
	topicRegex := "topic-1"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	r := reassignPartitions{Lister: mockLister, Partitioner: mockPartitioner, topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	r.reassignPartitions()
	mockPartitioner.AssertNotCalled(t, "ReassignPartitions", topics, brokerIds, batch, timeoutPerBatch, pollInterval, throttle)
	mockLister.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestReassignPartitions_Resume(t *testing.T){
	mockLister := &client.MockLister{}
	mockPartitioner := &client.MockPartitioner{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topics := []string{"topic-1","topic-3", "topic-2","topic-4"}
	toReassignTopics := []string{"topic-3", "topic-4"}
	topicRegex := "topic-."
	resume := "./testdata/reassign_partition_state"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockPartitioner.On("ReassignPartitions", toReassignTopics, brokerIds, batch, timeoutPerBatch, pollInterval, throttle).Return(nil).Times(1)
	r := reassignPartitions{Lister: mockLister, Partitioner: mockPartitioner, topics: topicRegex, brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, resumptionFile: resume}
	r.reassignPartitions()
	mockLister.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}
