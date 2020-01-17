package list

import (
	"errors"
	"os"
	"testing"

	"github.com/gojekfarm/kat/pkg/client"

	"bou.ke/monkey"
	"github.com/stretchr/testify/assert"

	"github.com/gojekfarm/kat/logger"
)

func init() {
	logger.SetDummyLogger()
}

func TestList_Success(t *testing.T) {
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{"topic-1": {}}, nil).Times(1)
	l := listTopic{Lister: mockLister, replicationFactor: 1}
	l.listTopic()
	mockLister.AssertExpectations(t)
}

func TestList_Empty(t *testing.T) {
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{}, nil).Times(1)
	l := listTopic{Lister: mockLister, replicationFactor: 1}
	l.listTopic()
	mockLister.AssertExpectations(t)
}

func TestList_Error(t *testing.T) {
	mockLister := &client.MockLister{}
	mockLister.On("List").Return(map[string]client.TopicDetail{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Lister: mockLister, replicationFactor: 1}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Success(t *testing.T) {
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{"topic-1"}, nil).Times(1)
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp"}
	l.listTopic()
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Empty(t *testing.T) {
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, nil).Times(1)
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp"}
	l.listTopic()
	mockLister.AssertExpectations(t)
}

func TestListLastWritten_Error(t *testing.T) {
	mockLister := &client.MockLister{}
	lastWrite := int64(123123)
	mockLister.On("ListLastWrittenTopics", lastWrite, "/tmp").Return([]string{}, errors.New("error")).Times(1)
	fakeExit := func(int) {
		panic("os.Exit called")
	}
	patch := monkey.Patch(os.Exit, fakeExit)
	defer patch.Unpatch()
	l := listTopic{Lister: mockLister, lastWrite: lastWrite, dataDir: "/tmp"}
	assert.PanicsWithValue(t, "os.Exit called", l.listTopic, "os.Exit was not called")
	mockLister.AssertExpectations(t)
}
