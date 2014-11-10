package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var UtilCommand = cobra.Command{
	Use: "util",
}

// Add any util-global flags below.

// ConfigPath is the value set by the --config flag. Commands are responsible
// for reading this config in if it isn't the empty string (unless they want to
// ignore it).
var ConfigPath string

func main() {
	UtilCommand.PersistentFlags().StringVarP(&ConfigPath,
		"config", "c", "", "path to a config file to load")

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Exiting with error: %v\n", r)
		}
	}()
	UtilCommand.Execute()
}
