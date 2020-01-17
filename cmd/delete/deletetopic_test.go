package delete

import (
	"errors"
	"os"
	"testing"

	"github.com/gojek/kat/pkg/client"

	"github.com/gojek/kat/logger"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func init() {
	logger.SetDummyLogger()
}

func TestDelete_ReturnWhenWhiteListAndBlackListAreEmpty(t *testing.T) {
	mockLister := &client.MockLister{}
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	d := deleteTopic{Lister: mockLister}
	assert.PanicsWithValue(t, "os.Exit called", d.deleteTopic, "os.Exit was not called")
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
}

func TestDelete_ReturnWhenWhiteListAndBlackListAreBothPassed(t *testing.T) {
	mockLister := &client.MockLister{}
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	d := deleteTopic{Lister: mockLister, topicWhitelist: "some", topicBlacklist: "some"}
	assert.PanicsWithValue(t, "os.Exit called", d.deleteTopic, "os.Exit was not called")
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicWhitelist: "test-1|test-2", userInput: mockUserInput}
	mockLister.On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	mockDeleter.On("Delete", topics).Return(nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicWhitelist: "test-1|test-2", userInput: mockUserInput}
	mockLister.On("ListOnly", d.topicWhitelist, true).Return(topics, nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockDeleter.AssertNotCalled(t, "Delete", topics)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicBlacklist: "test-1|test-2", userInput: mockUserInput}
	mockLister.On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	mockDeleter.On("Delete", topics).Return(nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsNotPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicBlacklist: "test-1|test-2", userInput: mockUserInput}
	mockLister.On("ListOnly", d.topicBlacklist, false).Return(topics, nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListLastWrittenTopics", mock.Anything, mock.Anything)
	mockDeleter.AssertNotCalled(t, "Delete", topics)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnConfirmation(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicWhitelist: "test-1|test-2", userInput: mockUserInput, lastWrite: 123}
	mockLister.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockDeleter.On("Delete", []string{"test-2"}).Return(nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesWhiteListedTopicsOnNo(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-1", "test-2"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicWhitelist: "test-1|test-2", userInput: mockUserInput, lastWrite: 123}
	mockLister.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockDeleter.AssertNotCalled(t, "Delete", topics)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnConfirmation(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-2"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicBlacklist: "test-1|test-2", userInput: mockUserInput, lastWrite: 123}
	mockLister.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockDeleter.On("Delete", []string{"test-3"}).Return(nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(true)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnNo(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicBlacklist: "test-1|test-2", userInput: mockUserInput, lastWrite: 123}
	mockLister.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, nil)
	mockUserInput.On("AskForConfirmation", mock.Anything).Return(false)

	d.deleteTopic()
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockDeleter.AssertNotCalled(t, "Delete", mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

func TestDelete_WhenLastWriteIsPassed_DeletesBlackListedTopicsOnError(t *testing.T) {
	mockLister := &client.MockLister{}
	mockDeleter := &client.MockDeleter{}
	mockUserInput := &MockUserInput{}
	topics := []string{"test-3", "test-4"}

	d := deleteTopic{Lister: mockLister, Deleter: mockDeleter, topicBlacklist: "test-1|test-2", userInput: mockUserInput, lastWrite: 123}
	mockLister.On("ListLastWrittenTopics", d.lastWrite, d.dataDir).Return(topics, errors.New("test"))
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()

	assert.PanicsWithValue(t, "os.Exit called", d.deleteTopic, "os.Exit was not called")
	mockLister.AssertNotCalled(t, "ListOnly", mock.Anything, mock.Anything)
	mockDeleter.AssertNotCalled(t, "Delete", mock.Anything)
	mockUserInput.AssertNotCalled(t, "AskForConfirmation", mock.Anything)
	mockLister.AssertExpectations(t)
	mockDeleter.AssertExpectations(t)
	mockUserInput.AssertExpectations(t)
}

type MockUserInput struct {
	mock.Mock
}

func (m *MockUserInput) AskForConfirmation(question string) bool {
	args := m.Called(question)
	return args.Bool(0)
}
