package mirror

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/gojekfarm/kat/logger"
	"github.com/gojekfarm/kat/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func init() {
	logger.SetupLogger("info")
}

func TestMirrorConfig_SourceClusterListError(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_SourceClusterGetConfigError(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return([]pkg.ConfigEntry{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_DestinationClusterListError(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_DestinationClusterGetConfigError(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	destinationCli.On("GetConfig", topicName).Return([]pkg.ConfigEntry{}, errors.New("error"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	assert.PanicsWithValue(t, "os.Exit called", m.mirrorTopicConfigs, "os.Exit was not called")

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsPresentAndConfigIsDifferent_Success(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val3",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}
	val2 := "val2"
	destinationCli.On("UpdateConfig", []string{topicName}, map[string]*string{"key2": &val2}, false).Return(nil)

	m.mirrorTopicConfigs()

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsDisabled_Noop(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{}, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything)
	destinationCli.AssertNotCalled(t, "UpdateConfig", mock.Anything, mock.Anything, mock.Anything)
	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsEnabled_Success(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{}, nil)
	destinationCli.On("Create", topicName, topic1Detail, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        true,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenTopicIsNotPresentAndCreateTopicIsEnabled_DryRun(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1Detail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1Detail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{}, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        true,
		increasePartitions: false,
		dryRun:             true,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.AssertNotCalled(t, "Create", mock.Anything, mock.Anything, mock.Anything)
	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagDisabled_Noop(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1SrcDetail := pkg.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: false,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagEnabled_Success(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1SrcDetail := pkg.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	destinationCli.On("CreatePartitions", topicName, topic1SrcDetail.NumPartitions, [][]int32{}, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenSourcePartitionIsLessThanDestinationPartition_Noop(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1SrcDetail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1DestDetail := pkg.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountIsDifferentAndFlagEnabled_DryRun(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1SrcDetail := pkg.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: true,
		dryRun:             true,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	destinationCli.AssertNotCalled(t, "CreatePartition", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}

func TestMirrorConfig_WhenPartitionCountAndConfigAreDifferent_Success(t *testing.T) {
	sourceCli := &pkg.MockTopicCli{}
	destinationCli := &pkg.MockTopicCli{}
	topic1SrcDetail := pkg.TopicDetail{
		NumPartitions:     2,
		ReplicationFactor: 1,
	}
	topic1DestDetail := pkg.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	topic1SrcConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val2",
	}}
	topic1DestConfigEntry := []pkg.ConfigEntry{{
		Name:  "key1",
		Value: "val1",
	}, {
		Name:  "key2",
		Value: "val3",
	}}
	topicName := "topic1"
	sourceCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1SrcDetail}, nil)
	sourceCli.On("GetConfig", topicName).Return(topic1SrcConfigEntry, nil)
	destinationCli.On("List").Return(map[string]pkg.TopicDetail{topicName: topic1DestDetail}, nil)
	destinationCli.On("GetConfig", topicName).Return(topic1DestConfigEntry, nil)
	destinationCli.On("CreatePartitions", topicName, topic1SrcDetail.NumPartitions, [][]int32{}, false).Return(nil)
	val2 := "val2"
	destinationCli.On("UpdateConfig", []string{topicName}, map[string]*string{"key2": &val2}, false).Return(nil)
	m := &mirror{
		sourceCli:          sourceCli,
		destinationCli:     destinationCli,
		createTopic:        false,
		increasePartitions: true,
		dryRun:             false,
		excludeConfigs:     nil,
	}

	m.mirrorTopicConfigs()

	sourceCli.AssertExpectations(t)
	destinationCli.AssertExpectations(t)
}
