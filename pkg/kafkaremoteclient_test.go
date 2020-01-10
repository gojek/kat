package pkg

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/gojekfarm/kat/logger"
	"github.com/stretchr/testify/assert"
)

func init() {
	logger.SetupLogger("info")
}

func TestKafkaRemoteClient_ListTopics_AllPartitionsStale(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	broker1Response.WriteString("topic-1 1\ntopic-2 1")
	broker2Response := bytes.Buffer{}
	broker2Response.WriteString("topic-1 1\ntopic-2 1")
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, nil)
	sshCli.On("DialAndExecute", "broker-2", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker2Response, nil)
	expectedTopicDetails := map[string]TopicDetail{
		"topic-1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
		},
		"topic-2": {
			NumPartitions:     1,
			ReplicationFactor: 2,
		},
	}
	apiClient.On("ListTopicDetails").Return(expectedTopicDetails, nil)

	topics, err := remoteClient.ListTopics(request)
	assert.NoError(t, err)
	sort.Strings(topics)
	assert.Equal(t, []string{"topic-1", "topic-2"}, topics)
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}

func TestKafkaRemoteClient_ListTopics_SomePartitionsAreStale(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	broker1Response.WriteString("topic-1 1\ntopic-2 1")
	broker2Response := bytes.Buffer{}
	broker2Response.WriteString("topic-1 1")
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, nil)
	sshCli.On("DialAndExecute", "broker-2", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker2Response, nil)
	expectedTopicDetails := map[string]TopicDetail{
		"topic-1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
		},
		"topic-2": {
			NumPartitions:     1,
			ReplicationFactor: 2,
		},
	}
	apiClient.On("ListTopicDetails").Return(expectedTopicDetails, nil)

	topics, err := remoteClient.ListTopics(request)
	assert.NoError(t, err)
	assert.Equal(t, []string{"topic-1"}, topics)
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}

func TestKafkaRemoteClient_ListTopics_ApiClientDoesNotReturnTopicDetail(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	broker1Response.WriteString("topic-1 1\ntopic-2 1")
	broker2Response := bytes.Buffer{}
	broker2Response.WriteString("topic-1 1\ntopic-2 1")
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, nil)
	sshCli.On("DialAndExecute", "broker-2", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker2Response, nil)
	expectedTopicDetails := map[string]TopicDetail{
		"topic-1": {
			NumPartitions:     1,
			ReplicationFactor: 2,
		},
	}
	apiClient.On("ListTopicDetails").Return(expectedTopicDetails, nil)

	topics, err := remoteClient.ListTopics(request)
	assert.NoError(t, err)
	assert.Equal(t, []string{"topic-1"}, topics)
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}

func TestKafkaRemoteClient_ListTopics_ApiClientReturnsError(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	broker1Response.WriteString("topic-1 1\ntopic-2 1")
	broker2Response := bytes.Buffer{}
	broker2Response.WriteString("topic-1 1\ntopic-2 1")
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, nil)
	sshCli.On("DialAndExecute", "broker-2", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker2Response, nil)
	apiClient.On("ListTopicDetails").Return(map[string]TopicDetail{}, errors.New("error"))

	topics, err := remoteClient.ListTopics(request)
	assert.Error(t, err)
	assert.Nil(t, topics)
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}

func TestKafkaRemoteClient_ListTopics_DialAndExecuteReturnsError(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, errors.New("error"))

	topics, err := remoteClient.ListTopics(request)
	assert.Error(t, err)
	assert.Nil(t, topics)
	apiClient.AssertNotCalled(t, "ListTopicDetails")
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}

func TestKafkaRemoteClient_ListTopics_DataIsNotReturnedInExpectedFormat(t *testing.T) {
	apiClient := &MockKafkaAPIClient{}
	sshCli := &MockSSHCli{}
	remoteClient, _ := NewKafkaRemoteClient(apiClient, sshCli)
	request := ListTopicsRequest{
		LastWritten: 123,
		DataDir:     "/tmp",
	}

	brokers := map[int]string{1: "broker-1:80", 2: "broker-2:80"}
	apiClient.On("ListBrokers").Return(brokers)
	broker1Response := bytes.Buffer{}
	broker1Response.WriteString("topic-1 1\ntopic-2 abc")
	sshCli.On("DialAndExecute", "broker-1", []shellCmd{NewCdCmd(request.DataDir), NewFindTopicsCmd(request.LastWritten, request.DataDir)}).Return(&broker1Response, nil)

	topics, err := remoteClient.ListTopics(request)
	assert.Error(t, err)
	assert.Nil(t, topics)
	apiClient.AssertNotCalled(t, "ListTopicDetails")
	apiClient.AssertExpectations(t)
	sshCli.AssertExpectations(t)
}
