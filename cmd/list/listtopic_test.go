package list

import (
	"errors"
	"os"
	"testing"

	"github.com/gojek/kat/pkg/client"
	"go.uber.org/goleak"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"

	"github.com/gojek/kat/logger"
)

func init() {
	logger.SetDummyLogger()
}

func TestListNoFlags(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(2)
	l := listTopic{Lister: mockLister, size: -1}
	topics, err := l.getTopicsFilteredByFlags()
	assert.ElementsMatch(t, topics, []string{"topic-1"})
	assert.Nil(t, err)
	l.listTopic()
	mockLister.AssertExpectations(t)
}

func TestList_Success(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {ReplicationFactor: 1}}, nil).Times(1)
	l := listTopic{Lister: mockLister, replicationFactor: 1, size: -1}
	topics, err := l.getTopicsFilteredByFlags()
	assert.ElementsMatch(t, topics, []string{"topic-1"})
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestList_Empty(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{}, nil).Times(1)
	l := listTopic{Lister: mockLister, replicationFactor: 1, size: -1}
	topics, err := l.getTopicsFilteredByFlags()
	assert.Empty(t, topics)
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestList_Error(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Lister: mockLister, replicationFactor: 1, size: -1}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Success(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}, "topic-2": {}}, nil).Times(1)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{"topic-1", "topic-2"}, nil).Times(1)
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp", size: -1}
	topics, err := l.getTopicsFilteredByFlags()
	assert.ElementsMatch(t, topics, []string{"topic-1", "topic-2"})
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Empty(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, nil).Times(1)
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp", size: -1}
	topics, err := l.getTopicsFilteredByFlags()
	assert.Empty(t, topics)
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Error(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp", size: -1}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockLister.AssertExpectations(t)
}

func TestListSize_Success(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	size := int64(10)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	mockLister.On("ListTopicWithSizeLessThanOrEqualTo", size).Return([]string{"topic-1"}, nil).Times(1)
	l := listTopic{Lister: mockLister, size: size}
	topics, err := l.getTopicsFilteredByFlags()
	assert.ElementsMatch(t, topics, []string{"topic-1"})
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestListSizeOnEmpty_Success(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	size := int64(10)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	mockLister.On("ListTopicWithSizeLessThanOrEqualTo", size).Return([]string{""}, nil).Times(1)
	l := listTopic{Lister: mockLister, size: size}
	topics, err := l.getTopicsFilteredByFlags()
	assert.Empty(t, topics)
	assert.Nil(t, err)
	mockLister.AssertExpectations(t)
}

func TestListSize_Failure(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockLister := &client.MockLister{}
	size := int64(10)
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	mockLister.On("ListTopicWithSizeLessThanOrEqualTo", size).Return([]string{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Lister: mockLister, size: size}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockLister.AssertExpectations(t)
}
