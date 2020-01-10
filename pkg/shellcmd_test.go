package pkg

import (
	"fmt"
	"testing"

	"github.com/gojekfarm/kat/util"
	"github.com/stretchr/testify/assert"
)

func TestFindTopicsCmd_Get_ReturnsFilesOlderThanDate(t *testing.T) {
	e := &util.Executor{}
	testDir := "/tmp/kat-test"
	e.Execute("mkdir", []string{"-p", testDir})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/abc-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/abc-2", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/def-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/def-2", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "202001020000", fmt.Sprintf("%s/xyz-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "202001020000", fmt.Sprintf("%s/xyz-2", testDir)})

	i := &util.IO{}
	i.WriteFile(fmt.Sprintf("%s/find_command.sh", testDir), NewFindTopicsCmd(1577836800, testDir).Get())
	resp, err := e.Execute("bash", []string{fmt.Sprintf("%s/find_command.sh", testDir)})

	assert.NoError(t, err)
	assert.Equal(t, "abc 2\ndef 2\n", resp.String())
	e.Execute("rm", []string{"-rf", testDir})
}

func TestFindTopicsCmd_Get_ReturnsEmptyWhenNoStaleFiles(t *testing.T) {
	e := &util.Executor{}
	testDir := "/tmp/kat-test"
	e.Execute("mkdir", []string{"-p", testDir})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/abc-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/abc-2", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/def-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "201912310000", fmt.Sprintf("%s/def-2", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "202001020000", fmt.Sprintf("%s/xyz-1", testDir)})
	e.Execute("touch", []string{"-a", "-m", "-t", "202001020000", fmt.Sprintf("%s/xyz-2", testDir)})

	i := &util.IO{}
	i.WriteFile(fmt.Sprintf("%s/find_command.sh", testDir), NewFindTopicsCmd(1577203200, testDir).Get())
	resp, err := e.Execute("bash", []string{fmt.Sprintf("%s/find_command.sh", testDir)})

	assert.NoError(t, err)
	assert.Equal(t, "", resp.String())
	e.Execute("rm", []string{"-rf", testDir})
}
