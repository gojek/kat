package list

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"

	"github.com/gojekfarm/kat/logger"

	"github.com/gojekfarm/kat/cmd/base"

	"github.com/gojekfarm/kat/pkg"
)

func init() {
	logger.SetupLogger("info")
}

func TestList_Success(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockTopicCli.On("List").Return(map[string]pkg.TopicDetail{"topic-1": {}}, nil).Times(1)
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: 1}
	l.listTopic()
	mockTopicCli.AssertExpectations(t)
}

func TestList_Empty(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockTopicCli.On("List").Return(map[string]pkg.TopicDetail{}, nil).Times(1)
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: 1}
	l.listTopic()
	mockTopicCli.AssertExpectations(t)
}

func TestList_Error(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockTopicCli.On("List").Return(map[string]pkg.TopicDetail{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, replicationFactor: 1}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockTopicCli.AssertExpectations(t)
}

func TestListLastWritten_Success(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	lastWrite := int64(123123)
	mockTopicCli.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{"topic-1"}, nil).Times(1)
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, lastWrite: lastWrite, dataDir: "/tmp"}
	l.listTopic()
	mockTopicCli.AssertExpectations(t)
}

func TestListLastWritten_Empty(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	lastWrite := int64(123123)
	mockTopicCli.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, nil).Times(1)
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, lastWrite: lastWrite, dataDir: "/tmp"}
	l.listTopic()
	mockTopicCli.AssertExpectations(t)
}

func TestListLastWritten_Error(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	lastWrite := int64(123123)
	mockTopicCli.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Cmd: base.Cmd{TopicCli: mockTopicCli}, lastWrite: lastWrite, dataDir: "/tmp"}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockTopicCli.AssertExpectations(t)
}
