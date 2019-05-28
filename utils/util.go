package utils

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"os"
	"strconv"
	"strings"
)

func GetAdminClient(cmd *cobra.Command) sarama.ClusterAdmin {
	addr := strings.Split(GetCmdArg(cmd, "broker-list"), ",")
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0

	admin, err := sarama.NewClusterAdmin(addr, cfg)
	if err != nil {
		fmt.Printf("Err on creating admin client: %v\n", err)
		os.Exit(1)
	}

	return admin
}

func GetCmdArg(cmd *cobra.Command, argName string) string {
	return cmd.Flags().Lookup(argName).Value.String()
}

func GetIntArg(cmd *cobra.Command, argName string) int {
	val, err := strconv.Atoi(GetCmdArg(cmd, argName))
	if err != nil {
		fmt.Printf("Error while retrieving int argument: %v\n", err)
		os.Exit(1)
	}
	return val
}