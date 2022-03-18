package main

import (
	"s3cli/cmd"

	log "github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

func init() {
	log.SetLevel(log.ErrorLevel)
	formatter := prefixed.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	}
	formatter.SetColorScheme(&prefixed.ColorScheme{
		PrefixStyle:    "blue+b",
		TimestampStyle: "white+h",
	})
	log.SetFormatter(&formatter)
	//log.SetFormatter(&log.JSONFormatter{})
	//log.SetReportCaller(true)
}

func main() {
	cmd.Execute()
}
