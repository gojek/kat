package admin

import (
	"errors"
	"os"
	"testing"

	"github.com/gojekfarm/kat/cmd/base"

	"bou.ke/monkey"
	"github.com/gojekfarm/kat/pkg"
	"github.com/stretchr/testify/assert"
)

func TestIncreaseReplicationFactor_Success(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	topics := []string{"topic1", "topic2"}
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	mockTopicCli.On("IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper).Return(nil).Times(1)
	i := increaseReplication{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	mockTopicCli.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_GetFailure(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	i := increaseReplication{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	assert.PanicsWithValue(t, "os.Exit called", i.increaseReplicationFactor, "os.Exit was not called")

	mockTopicCli.AssertNotCalled(t, "IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper)
	mockTopicCli.AssertExpectations(t)
}

func TestIncreaseReplicationFactor_NoMatch(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	var topics []string
	replicationFactor := 3
	numBrokers := 4
	batch := 1
	timeoutPerBatch := 1
	pollInterval := 1
	throttle := 100
	zookeeper := "zookeeper-host"
	topicRegex := "topic1|topic2"

	mockTopicCli.On("ListOnly", topicRegex, true).Return(topics, nil).Times(1)
	i := increaseReplication{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: replicationFactor, topics: topicRegex, numOfBrokers: numBrokers, batch: batch, timeoutPerBatchInS: timeoutPerBatch, pollIntervalInS: pollInterval, throttle: throttle, zookeeper: zookeeper}
	i.increaseReplicationFactor()
	mockTopicCli.AssertNotCalled(t, "IncreaseReplicationFactor", topics, replicationFactor, numBrokers, batch, timeoutPerBatch, pollInterval, throttle, zookeeper)
	mockTopicCli.AssertExpectations(t)
}
