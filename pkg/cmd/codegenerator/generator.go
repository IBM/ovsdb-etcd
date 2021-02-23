package main

import (
	"flag"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"k8s.io/klog"

	"github.com/roytman/ovsdb-etcd/pkg/codegenerator"
)

var (
	rootCmd = &cobra.Command{
		Use:   "generator",
		Short: "A code generator for OVSDB schema types",
		Long:  `generator creates goLang code for tables defined by the OVSDB schema.`,
		Run: func(cmd *cobra.Command, args []string) {
			codegenerator.Run()
		},
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	klog.InitFlags(nil)

	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("v"))
	pflag.CommandLine.AddGoFlag(flag.CommandLine.Lookup("logtostderr"))
	pflag.CommandLine.Set("logtostderr", "true")

	rootCmd.PersistentFlags().StringVarP(&codegenerator.SchemaFile, "schemaFile", "s", "", "input schema file")
	rootCmd.MarkPersistentFlagRequired("schemaFile")
	rootCmd.PersistentFlags().StringVarP(&codegenerator.OutputFile, "outputFile", "o", "types.go", "ouput file for the generated code, default is 'types.go'")
	rootCmd.PersistentFlags().StringVarP(&codegenerator.DestinationDir, "destinationDir", "d", ".", "base directory to store generated code, default is '.'")
	rootCmd.PersistentFlags().StringVarP(&codegenerator.PkgName, "packageName", "p", "", "the package of generated files, default is database name")
	rootCmd.PersistentFlags().StringVarP(&codegenerator.BasePackage, "basePackage", "b", "", "package with the base structures definitions, default is this 'repository/pkg/json'")
}

func initConfig() {

	viper.AutomaticEnv()

}

func main() {
	Execute()
}
