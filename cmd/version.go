package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:     "version [flags]",
	Aliases: []string{"info"},
	Short:   "Print the version",
	Long:    `Print the semantic version of this program.`,
	Run:     execShowVersion,
}

func execShowVersion(cmd *cobra.Command, args []string) {
	fmt.Println("version 1.0.1")
}
