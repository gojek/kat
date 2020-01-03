package util_test

import (
	"github.com/gojekfarm/kat/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListFilterHandlesEmptyList(t *testing.T) {
	var emptyList []string
	filteredList, err := util.Filter(emptyList, "test", true)

	assert.NoError(t, err)
	assert.Equal(t, emptyList, filteredList)
}

func TestListFilterReturnsWhitelistedResult(t *testing.T) {
	filteredList, err := util.Filter([]string{"test-1", "test-2", "something"}, "test.*", true)

	assert.NoError(t, err)
	assert.Equal(t, []string{"test-1", "test-2"}, filteredList)
}

func TestListFilterReturnsBlacklistedResult(t *testing.T) {
	filteredList, err := util.Filter([]string{"test-1", "test-2", "something"}, "test.*", false)

	assert.NoError(t, err)
	assert.Equal(t, []string{"something"}, filteredList)
}

