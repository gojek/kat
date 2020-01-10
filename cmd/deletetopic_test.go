package cmd

import (
	"errors"
	"os"
	"testing"

	"bou.ke/monkey"
	"github.com/gojekfarm/kat/pkg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDelete_ReturnWhenWhiteListAndBlackListAreEmpty(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}}
	assert.PanicsWithValue(t, "os.Exit called", d.deleteTopic, "os.Exit was not called")
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
}

func TestDelete_ReturnWhenWhiteListAndBlackListAreBothPassed(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicWhitelist: "some", topicBlacklist: "some"}
	assert.PanicsWithValue(t, "os.Exit called", d.deleteTopic, "os.Exit was not called")
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicWhitelist: "test-1|test-2", io: mockIo}
	mockTopicCli.On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	mockTopicCli.On("Delete", topics).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicWhitelist: "test-1|test-2", io: mockIo}
	mockTopicCli.On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "Delete", topics)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicBlacklist: "test-1|test-2", io: mockIo}
	mockTopicCli.On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	mockTopicCli.On("Delete", topics).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicBlacklist: "test-1|test-2", io: mockIo}
	mockTopicCli.On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "Delete", topics)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicWhitelist: "test-1|test-2", io: mockIo, lastWrite: 123}
	mockTopicCli.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockTopicCli.On("Delete", []string{"test-2"}).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicWhitelist: "test-1|test-2", io: mockIo, lastWrite: 123}
	mockTopicCli.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "Delete", topics)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	mockTopicCli.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockTopicCli.On("Delete", []string{"test-3"}).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	mockTopicCli.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "Delete", mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnError(t *testing.T) {
	mockTopicCli := &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{BaseCmd: BaseCmd{TopicCli: mockTopicCli}, topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	mockTopicCli.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, errors.New("test"))

	d.deleteTopic()
	mockTopicCli.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockTopicCli.AssertNotCalled(t, "Delete", mock.Anything)
	mockIo.AssertNotCalled(t, "AskForConfirmation", mock.Anything)
	mockTopicCli.AssertExpectations(t)
	mockIo.AssertExpectations(t)
}
