package cmd

import (
	"log"
	"os"
	"path/filepath"
	s3base "s3cli/base"
	"strings"

	cobra "github.com/spf13/cobra"
)

var (
	uploadFlags = struct {
		keyToPathDelimiter string
		recursive          bool
		keyMode            string
	}{
		keyToPathDelimiter: "/",
		recursive:          false,
		keyMode:            "B",
	}
	uploadCmd = &cobra.Command{
		Use:        "up [flags] <bucket-name> <local-path> [key-prefix]",
		Aliases:    []string{"upload", "put"},
		Short:      "uploads objects",
		Long:       `uploads objects to S3.`,
		Run:        up,
		Args:       cobra.MinimumNArgs(2),
		ArgAliases: []string{"bucket", "local-path", "key-prefix"},
	}
)

func init() {
	uploadCmd.PersistentFlags().StringVarP(&uploadFlags.keyToPathDelimiter, "delimiter", "d", uploadFlags.keyToPathDelimiter, "delimiter used to convert object keys to filesystem paths")
	uploadCmd.PersistentFlags().BoolVarP(&uploadFlags.recursive, "recursive", "r", uploadFlags.recursive, "uploads directories and their contents recursively")
	uploadCmd.PersistentFlags().StringVarP(&uploadFlags.keyMode, "use-keymode", "k", uploadFlags.keyMode, "mode to translate filenames if object key is not specified explicitly - allowed values are: B (basename), A (absolute) or R (relative)")
	rootCmd.AddCommand(uploadCmd)
}

func up(cmd *cobra.Command, args []string) {
	bucket := FindBucket(args[0])

	sourceBase := args[1]
	fileInfo, err := os.Stat(sourceBase)
	s3base.CheckIfError(1, err)

	key := ""
	if len(args) > 2 {
		key = args[2]
	}

	if fileInfo.IsDir() {
		if !uploadFlags.recursive {
			log.Fatalf("file %s is a directory (use recursive flag)", sourceBase)
			return
		}
		err := filepath.Walk(sourceBase,
			func(p string, fi os.FileInfo, err error) error {
				s3base.CheckIfError(2, err)
				if !fi.IsDir() {
					f, err := os.Open(p)
					s3base.CheckIfError(3, err)
					defer f.Close()
					return bucket.Upload(createKey(key, p, sourceBase), f)
				}
				return nil
			})
		s3base.CheckIfError(4, err)
	} else {
		f, err := os.Open(sourceBase)
		s3base.CheckIfError(5, err)
		defer f.Close()
		err = bucket.Upload(createKey(key, sourceBase, filepath.Dir(sourceBase)), f)
		s3base.CheckIfError(6, err)
	}
}

func createKey(keyPrefix string, source string, sourceBasePath string) string {
	if keyPrefix == "" || uploadFlags.recursive {
		a, err := filepath.Abs(source)
		s3base.CheckIfError(1, err)
		switch uploadFlags.keyMode {
		case "b":
			fallthrough
		case "B":
			return keyPrefix + filepath.Base(a)
		case "A":
			fallthrough
		case "a":
			return keyPrefix + strings.ReplaceAll(a, string(os.PathSeparator), uploadFlags.keyToPathDelimiter)
		case "R":
			fallthrough
		case "r":
			return keyPrefix + strings.ReplaceAll(strings.TrimPrefix(a, sourceBasePath), string(os.PathSeparator), uploadFlags.keyToPathDelimiter)
		default:
			log.Fatalf("invalid key mode %s specified!", uploadFlags.keyMode)
		}
	}
	return keyPrefix
}
