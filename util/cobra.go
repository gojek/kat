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

func (u *CobraUtil) GetAdminClient(argName string) sarama.ClusterAdmin {
	addr := strings.Split(u.GetCmdArg(argName), ",")
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0

	admin, err := sarama.NewClusterAdmin(addr, cfg)
	if err != nil {
		fmt.Printf("Err on creating admin client: %v\n", err)
		os.Exit(1)
	}

	return admin
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
