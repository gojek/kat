package admin

import (
	"errors"
	"os"
	"testing"

	"github.com/gojekfarm/kat/pkg/client"
	"github.com/stretchr/testify/mock"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
)

func TestIncreaseReplicationFactor_Success(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDescriber := &client.MockDescriber{}
	mockPartitioner := &client.MockPartitioner{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topicRegex := "topic1|topic2"
	topicMetadata := []*client.TopicMetadata{{Name: "topic1"}, {Name: "topic2"}}

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockDescriber.On("Describe", topics).Return(topicMetadata, nil).Times(1)
	mockPartitioner.On("IncreaseReplication", topicMetadata, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle).Return(nil).Times(1)
	i := increaseReplication{Lister: mockLister, Describer: mockDescriber, Partitioner: mockPartitioner, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	i.increaseReplicationFactor()
	mockLister.AssertExpectations(t)
	mockDescriber.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_ListFailure(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDescriber := &client.MockDescriber{}
	mockPartitioner := &client.MockPartitioner{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topicRegex := "topic1|topic2"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	i := increaseReplication{Lister: mockLister, Describer: mockDescriber, Partitioner: mockPartitioner, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	assert.PanicsWithValue(t, "os.Exit called", i.increaseReplicationFactor, "os.Exit was not called")

	mockDescriber.AssertNotCalled(t, "Describe", mock.Anything)
	mockPartitioner.AssertNotCalled(t, "IncreaseReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDescriber.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_DescribeFailure(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDescriber := &client.MockDescriber{}
	mockPartitioner := &client.MockPartitioner{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topicRegex := "topic1|topic2"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockDescriber.On("Describe", topics).Return([]*client.TopicMetadata{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	i := increaseReplication{Lister: mockLister, Describer: mockDescriber, Partitioner: mockPartitioner, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	assert.PanicsWithValue(t, "os.Exit called", i.increaseReplicationFactor, "os.Exit was not called")

	mockPartitioner.AssertNotCalled(t, "IncreaseReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDescriber.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_IncreaseReplicationFailure(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDescriber := &client.MockDescriber{}
	mockPartitioner := &client.MockPartitioner{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topicRegex := "topic1|topic2"
	topicMetadata := []*client.TopicMetadata{{Name: "topic1"}, {Name: "topic2"}}

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockDescriber.On("Describe", topics).Return(topicMetadata, nil).Times(1)
	mockPartitioner.On("IncreaseReplication", topicMetadata, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle).Return(errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	i := increaseReplication{Lister: mockLister, Describer: mockDescriber, Partitioner: mockPartitioner, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	assert.PanicsWithValue(t, "os.Exit called", i.increaseReplicationFactor, "os.Exit was not called")

	mockLister.AssertExpectations(t)
	mockDescriber.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_NoMatch(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDescriber := &client.MockDescriber{}
	mockPartitioner := &client.MockPartitioner{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	topicRegex := "topic1|topic2"

	mockLister.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	i := increaseReplication{Lister: mockLister, Describer: mockDescriber, Partitioner: mockPartitioner, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle}
	i.increaseReplicationFactor()
	mockDescriber.AssertNotCalled(t, "Describe", mock.Anything)
	mockPartitioner.AssertNotCalled(t, "IncreaseReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDescriber.AssertExpectations(t)
	mockPartitioner.AssertExpectations(t)
}
