package base

import (
	"fmt"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

func Exec(cmd string, args ...string) {
	p := exec.Command(cmd, args...)
	stdout, err := p.CombinedOutput()
	if err != nil {
		log.Error("error executing command: ", cmd)
		log.Fatal(string(stdout))
	} else {
		log.Info(string(stdout))
	}
}

func CheckIfError(rc int, e error) {
	if e != nil {
		log.Fatalf("%s", e)
		os.Exit(rc)
	}
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func ByteCountIEC(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
