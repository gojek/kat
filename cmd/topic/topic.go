package topic

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var TopicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Admin commands on topics",
}

func init() {
	TopicCmd.AddCommand(listCmd)
}

func getReplicationFactor(cmd *cobra.Command) int {
	flags := cmd.Flags()
	replicationFactor, err := flags.GetInt("replication-factor")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return replicationFactor
}