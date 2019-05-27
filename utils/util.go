package utils

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func GetAdminClient(cmd *cobra.Command) sarama.ClusterAdmin {
	addr := getBrokerList(cmd)
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_0_0_0

	admin, err := sarama.NewClusterAdmin(addr, cfg)
	if err != nil {
		fmt.Printf("Err on creating admin client: %v\n", err)
		os.Exit(1)
	}

	return admin
}

func getBrokerList(cmd *cobra.Command) []string {
	flags := cmd.Flags()
	brokerList, err := flags.GetString("broker-list")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.Split(brokerList, ",")
}
