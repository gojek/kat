package util

import (
	"os"
	"strconv"
	"strings"

	"github.com/gojekfarm/kat/logger"

	"github.com/spf13/cobra"
)

type CobraUtil struct {
	cmd *cobra.Command
}

func NewCobraUtil(cmd *cobra.Command) *CobraUtil {
	return &CobraUtil{cmd: cmd}
}

func (u *CobraUtil) GetStringArg(argName string) string {
	lookup := u.cmd.Flags().Lookup(argName)
	if lookup == nil {
		return ""
	}
	return lookup.Value.String()
}

func (u *CobraUtil) GetIntArg(argName string) int {
	strVal := u.GetStringArg(argName)
	if strVal == "" {
		return 0
	}

	val, err := strconv.Atoi(strVal)
	if err != nil {
		logger.Errorf("Error while retrieving int argument: %v\n", err)
		os.Exit(1)
	}
	return val
}

func (u *CobraUtil) GetBoolArg(argName string) bool {
	strVal := u.GetStringArg(argName)
	val, err := strconv.ParseBool(strVal)
	if err != nil {
		logger.Errorf("Error while retrieving bool argument: %v\n", err)
		os.Exit(1)
	}
	return val

}

func (u *CobraUtil) GetStringSliceArg(argName string) []string {
	stringSlice, err := u.cmd.Flags().GetStringSlice(argName)
	if err != nil {
		logger.Errorf("Error while retrieving string slice argument: %v\n", err)
		os.Exit(1)
	}
	return stringSlice

}

func (u *CobraUtil) GetTopicNames() []string {
	return strings.Split(u.GetStringArg("topics"), ",")
}
