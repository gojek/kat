package pkg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestListFilter_HandlesEmptyList(t *testing.T) {
	var emptyList []string
	l := ListUtil{emptyList}
	filteredList, err := l.Filter("test", true)

	assert.NoError(t, err)
	assert.Equal(t, emptyList, filteredList)
}

func TestListFilter_ReturnsWhitelistedResult(t *testing.T) {
	l := ListUtil{[]string{"test-1", "test-2", "something"}}
	filteredList, err := l.Filter("test.*", true)

	assert.NoError(t, err)
	assert.Equal(t, []string{"test-1", "test-2"}, filteredList)
}

func TestListFilter_ReturnsBlacklistedResult(t *testing.T) {
	l := ListUtil{[]string{"test-1", "test-2", "something"}}
	filteredList, err := l.Filter("test.*", false)

	assert.NoError(t, err)
	assert.Equal(t, []string{"something"}, filteredList)
}

func TestListContains_ReturnsTrueIfPresent(t *testing.T) {
	l := ListUtil{[]string{"test-1", "test-2", "something"}}

	assert.True(t, l.Contains("test-2"))
}

func TestListContains_ReturnsFalseIfAbsent(t *testing.T) {
	l := ListUtil{[]string{"test-1", "test-2", "something"}}

	assert.False(t, l.Contains("test-3"))
}

func TestListContains_ReturnsFalseForEmptyList(t *testing.T) {
	l := ListUtil{[]string{}}

	assert.False(t, l.Contains("test-3"))
}
