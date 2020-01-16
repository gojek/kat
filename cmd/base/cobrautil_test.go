package base

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

var testCmd = &cobra.Command{
	Use: "test",
}

func init() {
	testCmd.PersistentFlags().StringP("key1", "", "", "test key 1")
	testCmd.PersistentFlags().StringP("key2", "", "", "test key 2")
	testCmd.PersistentFlags().StringP("topics", "t", "", "topics")
	testCmd.PersistentFlags().Bool("increase-partitions", false, "partitions")
}

func TestCobraUtil_GetCmdArgReturnsValue(t *testing.T) {
	testCmd.SetArgs([]string{
		"--key1=val1",
	})
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	value := util.GetStringArg("key1")

	assert.Equal(t, "val1", value)
}

func TestCobraUtil_GetCmdArgReturnsEmptyStringWhenNotPresent(t *testing.T) {
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	value := util.GetStringArg("key2")

	assert.Equal(t, "", value)
}

func TestCobraUtil_GetIntArgReturnsIntValue(t *testing.T) {
	testCmd.SetArgs([]string{
		"--key1=1",
	})
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	value := util.GetIntArg("key1")

	assert.Equal(t, 1, value)
}

func TestCobraUtil_GetIntArgReturnsZeroWhenNotPresent(t *testing.T) {
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	value := util.GetIntArg("key2")

	assert.Equal(t, 0, value)
}

func TestCobraUtil_GetTopicNamesReturnsArrayWithValue(t *testing.T) {
	testCmd.SetArgs([]string{
		"--topics=topic1,topic2",
	})
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	value := util.GetTopicNames()

	assert.Equal(t, 2, len(value))
	assert.Equal(t, "topic1", value[0])
	assert.Equal(t, "topic2", value[1])
}

func TestCobraUtil_GetBoolArgsReturnsTrue(t *testing.T) {
	testCmd.SetArgs([]string{
		"--increase-partitions",
	})
	testCmd.Execute()

	util := NewCobraUtil(testCmd)
	val := util.GetBoolArg("increase-partitions")

	assert.Equal(t, true, val)
}
