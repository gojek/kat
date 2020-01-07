package pkg

import (
	"fmt"
	"time"
)

type shellCmd interface {
	Get() string
}

type cdCmd struct {
	dir string
}

func NewCdCmd(dir string) *cdCmd {
	return &cdCmd{dir: dir}
}

func (c *cdCmd) Get() string {
	return fmt.Sprintf("cd %s", c.dir)
}

type findTopicsCmd struct {
	lastWritten                int64
	dataDir                    string
	findLastWrittenDirectories string
	removePathPrefix           string
	removePartitionSuffix      string
	sortAndCount               string
	reorder                    string
}

func NewFindTopicsCmd(lastWritten int64, dataDir string) *findTopicsCmd {
	return &findTopicsCmd{
		lastWritten:                lastWritten,
		dataDir:                    dataDir,
		findLastWrittenDirectories: "find %s -maxdepth 1 -not -path \"*/\\.*\" -not -newermt \"%s\"",
		removePathPrefix:           "xargs -I{} echo {} | rev | cut -d / -f1 | rev",
		removePartitionSuffix:      "xargs -I{} echo {} | rev | cut -d - -f2- | rev",
		sortAndCount:               "sort | uniq -c",
		reorder:                    "awk '{ print $2 \" \" $1}'",
	}
}

func (f *findTopicsCmd) Get() string {
	dateTime := time.Unix(f.lastWritten, 0)
	return fmt.Sprintf("%s | %s | %s | %s | %s",
		fmt.Sprintf(f.findLastWrittenDirectories, f.dataDir, dateTime.UTC().Format(time.UnixDate)),
		f.removePathPrefix,
		f.removePartitionSuffix,
		f.sortAndCount,
		f.reorder)
}
