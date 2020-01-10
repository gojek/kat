package cmd

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetupLogger("info")
}

func TestReassignPartitions_Success(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topics := []string{"topic-1"}
	topicRegex := "topic-1"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockTopicCli.On("ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper).Return(nil).Times(1)
	r := reassignPartitions{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: topicRegex, brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	r.reassignPartitions()
	mockTopicCli.AssertExpectations(t)
}

func TestReassignPartitions_ListFailure(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	var topics []string
	topicRegex := "topic-1"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	r := reassignPartitions{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	assert.PanicsWithValue(t, "os.Exit called", r.reassignPartitions, "os.Exit was not called")

	mockTopicCli.AssertNotCalled(t, "ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper)
	mockTopicCli.AssertExpectations(t)
}

func TestReassignPartitions_NoMatch(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	brokerIds := "1,2"
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	var topics []string
	topicRegex := "topic-1"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	r := reassignPartitions{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topics: "topic-1", brokerIds: brokerIds, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	r.reassignPartitions()
	mockTopicCli.AssertNotCalled(t, "ReassignPartitions", topics, batch, timeoutPerBatch, pollInterval, throttle, brokerIds, zookeeper)
	mockTopicCli.AssertExpectations(t)
}
