package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
)

type CobraUtil struct {
	cmd *cobra.Command
}

func NewCobraUtil(cmd *cobra.Command) *CobraUtil {
	return &CobraUtil{cmd: cmd}
}

func (u *CobraUtil) GetSaramaClient(argName string) (sarama.ClusterAdmin, sarama.Client) {
	addr := strings.Split(u.GetCmdArg(argName), ",")
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0

	admin, err := sarama.NewClusterAdmin(addr, cfg)
	if err != nil {
		fmt.Printf("Err on creating admin for %s: %v\n", addr, err)
		os.Exit(1)
	}

	client, err := sarama.NewClient(addr, cfg)
	if err != nil {
		fmt.Printf("Err on creating client for %s: %v\n", addr, err)
		os.Exit(1)
	}

	return admin, client
}

func (u *CobraUtil) GetCmdArg(argName string) string {
	lookup := u.cmd.Flags().Lookup(argName)
	if lookup == nil {
		return ""
	}
	return lookup.Value.String()
}

func (u *CobraUtil) GetIntArg(argName string) int {
	strVal := u.GetCmdArg(argName)
	if strVal == "" {
		return 0
	}

	val, err := strconv.Atoi(strVal)
	if err != nil {
		fmt.Printf("Error while retrieving int argument: %v\n", err)
		os.Exit(1)
	}
	return val
}

func (u *CobraUtil) GetStringSliceArg(argName string) []string {
	stringSlice, err := u.cmd.Flags().GetStringSlice(argName)
	if err != nil {
		fmt.Printf("Error while retrieving string slice argument: %v\n", err)
		os.Exit(1)
	}
	return stringSlice

}

func (u *CobraUtil) GetStringSet(argName string) map[string]interface{} {
	stringSlice := u.GetStringSliceArg(argName)
	stringSet := make(map[string]interface{})
	for _, value := range stringSlice {
		stringSet[strings.TrimSpace(value)] = true
	}
	return stringSet
}

func (u *CobraUtil) GetTopicNames() []string {
	return strings.Split(u.GetCmdArg("topics"), ",")
}

func (u *CobraUtil) GetBoolArg(argName string) bool {
	strVal := u.GetCmdArg(argName)
	val, err := strconv.ParseBool(strVal)
	if err != nil {
		fmt.Printf("Error while retrieving bool argument: %v\n", err)
		os.Exit(1)
	}
	return val

}
