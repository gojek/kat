package mirror

import (
	"errors"
	"os"
	"testing"

	"github.com/gojek/kat/pkg/client"

	"bou.ke/monkey"
	"github.com/gojek/kat/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func init() {
	logger.SetDummyLogger()
}

type mockCreateOrUpdate struct {
	client.MockCreator
	client.MockLister
	client.MockDescriber
	client.MockConfigurer
}

func (m *mockCreateOrUpdate) assertExpectations(t *testing.T) {
	m.MockCreator.AssertExpectations(t)
	m.MockLister.AssertExpectations(t)
	m.MockDescriber.AssertExpectations(t)
	m.MockConfigurer.AssertExpectations(t)
}

func TestMirrorConfig_SourceClusterListError(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     nil,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.assertExpectations(t)
}

func TestMirrorConfig_SourceClusterGetConfigError(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return([]client.ConfigEntry{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     nil,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.assertExpectations(t)
}

func TestMirrorConfig_DestinationClusterListError(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_DestinationClusterGetConfigError(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return([]client.ConfigEntry{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsPresentAndConfigIsDifferent_Success(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val3",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	val2 := "val2"
	destinationCli.MockConfigurer.On("UpdateConfig", []string{topicName}, map[string]*string{"key2": &val2}, false).Return(nil)

	m.mirrorTopicConfigs()

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsDisabled_Noop(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{}, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.MockCreator.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything)
	destinationCli.MockConfigurer.AssertNotCalled(t, "UpdateConfig", mock.Anything, mock.Anything, mock.Anything)
	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsEnabled_Success(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{}, nil)
	destinationCli.MockCreator.On("Create", topicName, topic1Detail, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       true,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsEnabled_DryRun(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1Detail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{}, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       true,
		increasePartitions: false,
		dryRun:             true,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.MockCreator.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything)
	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagDisabled_Noop(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1SrcDetail := client.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.MockCreator.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagEnabled_Success(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1SrcDetail := client.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	destinationCli.MockCreator.On("CreatePartitions", topicName, topic1SrcDetail.NumPartitions, [][]int32{}, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenSourcePartitionIsLessThanDestinationPartition_Noop(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1SrcDetail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1DestDetail := client.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.MockCreator.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagEnabled_DryRun(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1SrcDetail := client.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: true,
		dryRun:             true,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.MockCreator.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountAndConfigAreDifferent_Success(t *testing.T) {
	sourceCli := &mockCreateOrUpdate{}
	destinationCli := &mockCreateOrUpdate{}
	topic1SrcDetail := client.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := client.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []client.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val3",
	}}
	topicName := "topic1"
	sourceCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.MockConfigurer.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.MockLister.On("List").Return(map[string]client.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.MockConfigurer.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	destinationCli.MockCreator.On("CreatePartitions", topicName, topic1SrcDetail.NumPartitions, [][]int32{}, false).Return(nil)
	val2 := "val2"
	destinationCli.MockConfigurer.On("UpdateConfig", []string{topicName}, map[string]*string{"key2": &val2}, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopics:       false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.assertExpectations(t)
	destinationCli.assertExpectations(t)
}
