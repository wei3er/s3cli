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

	file, err := os.Open(args[1])
	s3base.CheckIfError(1, err)
	defer file.Close()
	basePath, err := filepath.Abs(file.Name())
	s3base.CheckIfError(2, err)
	fileInfo, err := file.Stat()
	s3base.CheckIfError(3, err)

	key := ""
	if len(args) > 2 {
		key = args[2]
	}

	if !fileInfo.IsDir() {
		bucket.Upload(createKey(key, basePath, filepath.Dir(basePath)), file)
	} else {
		if !uploadFlags.recursive {
			log.Fatalf("file %s is a directory (use recursive flag)", file.Name())
		}

		err := filepath.Walk(file.Name(),
			func(p string, fi os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !fi.IsDir() {
					f, err := os.Open(p)
					if err != nil {
						return err
					}
					defer f.Close()
					bucket.Upload(createKey(key, p, basePath), f)
				}
				return nil
			})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func createKey(key string, file string, basePath string) string {
	if key == "" || uploadFlags.recursive {
		a, err := filepath.Abs(file)
		s3base.CheckIfError(1, err)
		switch uploadFlags.keyMode {
		case "b":
			fallthrough
		case "B":
			return key + filepath.Base(a)
		case "A":
			fallthrough
		case "a":
			return key + strings.ReplaceAll(a, string(os.PathSeparator), uploadFlags.keyToPathDelimiter)
		case "R":
			fallthrough
		case "r":
			return key + strings.ReplaceAll(strings.TrimPrefix(a, basePath), string(os.PathSeparator), uploadFlags.keyToPathDelimiter)
		default:
			log.Fatalf("invalid key mode %s specified!", uploadFlags.keyMode)
		}
	}
	return key
}
