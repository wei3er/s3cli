package cmd

import (
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
	cobra "github.com/spf13/cobra"
	viper "github.com/spf13/viper"
)

var (
	logLevel   string = "ERROR"
	configFile string
	rootCmd    = &cobra.Command{
		Use:   "s3",
		Short: "S3",
		Long:  `Various operations with S3 buckets.`,
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initRootConfig)
	rootCmd.PersistentFlags().StringVarP(&configFile, "config", "C", configFile, "config file")
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "L", logLevel, "set log level, i.e. one of DEBUG, INFO, WARN, ERROR")
}

func initRootConfig() {
	setupLogging()
	readConfiguration()
}

func setupLogging() {
	switch strings.ToUpper(logLevel) {
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	default:
		log.Fatal("unsupported log level specified: ", logLevel)
	}
	log.Debug("log level is set to ", logLevel)
}

func readConfiguration() {
	viper.SetConfigName("buckets")
	//viper.SetConfigType("yaml")

	if configFile == "" {
		viper.AddConfigPath("$HOME/.s3")
		viper.AddConfigPath("/etc/s3")
		path, err := os.Executable()
		if err != nil {
			log.Error("can no determine executable path.")
		}
		log.Debug("executable ist located at ", path)
		viper.AddConfigPath(filepath.Dir(path) + "/etc")
		viper.AddConfigPath(".")

	} else {
		viper.SetConfigFile(configFile)
	}

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("error reading config file: %w", err)
	}
	log.Info("using config file at ", viper.ConfigFileUsed())
}
