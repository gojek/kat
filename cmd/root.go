package cmd

import (
	"fmt"
	"github.com/gojekfarm/kat/util"
	"os"

	"github.com/gojekfarm/kat/cmd/mirror"
	"github.com/spf13/cobra"
)

var Cobra *util.CobraUtil

var cliCmd = &cobra.Command{
	Use:     "./kat",
	Short:   "Tool used for admin activities against specified kafka brokers",
	Version: fmt.Sprintf("%s (Commit: %s)", "0.0.1", "n/a"),
}

func LoadCobra(cmd *cobra.Command, args []string) {
	Cobra = util.NewCobraUtil(cmd)
}

func ClearCobra(cmd *cobra.Command, args []string) {
	Cobra = nil
}

func init() {
	cobra.OnInitialize()
	cliCmd.AddCommand(topicCmd)
	cliCmd.AddCommand(mirror.MirrorCmd)
}

func Execute() {
	if err := cliCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
