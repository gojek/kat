package cmd

import (
	"errors"
	"github.com/gojekfarm/kat/pkg"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestDelete_ReturnWhenWhiteListAndBlackListAreEmpty(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	d := deleteTopic{}
	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
}

func TestDelete_ReturnWhenWhiteListAndBlackListAreBothPassed(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	d := deleteTopic{topicWhitelist: "some", topicBlacklist: "some"}
	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{topicWhitelist: "test-1|test-2", io: mockIo}
	TopicCli.(*pkg.MockTopicCli).On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	TopicCli.(*pkg.MockTopicCli).On("Delete", topics).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{topicWhitelist: "test-1|test-2", io: mockIo}
	TopicCli.(*pkg.MockTopicCli).On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "Delete", topics)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{topicBlacklist: "test-1|test-2", io: mockIo}
	TopicCli.(*pkg.MockTopicCli).On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	TopicCli.(*pkg.MockTopicCli).On("Delete", topics).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{topicBlacklist: "test-1|test-2", io: mockIo}
	TopicCli.(*pkg.MockTopicCli).On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "Delete", topics)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{topicWhitelist: "test-1|test-2", io: mockIo, lastWrite: 123}
	TopicCli.(*pkg.MockTopicCli).On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	TopicCli.(*pkg.MockTopicCli).On("Delete", []string{"test-2"}).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{topicWhitelist: "test-1|test-2", io: mockIo, lastWrite: 123}
	TopicCli.(*pkg.MockTopicCli).On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "Delete", topics)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	TopicCli.(*pkg.MockTopicCli).On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	TopicCli.(*pkg.MockTopicCli).On("Delete", []string{"test-3"}).Return(nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	TopicCli.(*pkg.MockTopicCli).On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockIo.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "Delete", mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}


func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnError(t *testing.T) {
	clearTopicCli(nil, nil)
	TopicCli = &pkg.MockTopicCli{}
	mockIo := &pkg.MockIo{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{topicBlacklist: "test-1|test-2", io: mockIo, lastWrite: 123}
	TopicCli.(*pkg.MockTopicCli).On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, errors.New("test"))

	d.deleteTopic()
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertNotCalled(t, "Delete", mock.Anything)
	mockIo.AssertNotCalled(t, "AskForConfirmation", mock.Anything)
	TopicCli.(*pkg.MockTopicCli).AssertExpectations(t)
	mockIo.AssertExpectations(t)
}
