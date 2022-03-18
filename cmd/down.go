package cmd

import (
	"os"
	s3base "s3cli/base"
	s3sss "s3cli/sss"
	"strings"

	cobra "github.com/spf13/cobra"
)

var (
	downloadFlags = struct {
		keyToPathDelimiter string
		recursive          bool
		force              bool
		fetchSize          int
		//parallel  int
	}{
		keyToPathDelimiter: "/",
		recursive:          false,
		force:              false,
		fetchSize:          1000,
		//parallel:  3,
	}
	downloadCmd = &cobra.Command{
		Use:        "down [flags] <bucket-name> <key-prefix> [local-path]",
		Aliases:    []string{"download", "get"},
		Short:      "downloads objects",
		Long:       `downloads objects to S3.`,
		Run:        download,
		Args:       cobra.MinimumNArgs(2),
		ArgAliases: []string{"bucket", "key-prefix", "local-path"},
	}
)

func init() {
	downloadCmd.PersistentFlags().StringVarP(&downloadFlags.keyToPathDelimiter, "delimiter", "d", downloadFlags.keyToPathDelimiter, "delimiter used to convert object keys to filesystem paths")
	downloadCmd.PersistentFlags().BoolVarP(&downloadFlags.recursive, "recursive", "r", downloadFlags.recursive, "remove directories and their contents recursively")
	downloadCmd.PersistentFlags().BoolVarP(&downloadFlags.force, "force", "f", downloadFlags.force, "overwrite an existing destination file")
	downloadCmd.PersistentFlags().IntVarP(&downloadFlags.fetchSize, "fetch-size", "n", downloadFlags.fetchSize, "fetch objects in batches of this size")
	//downloadCmd.PersistentFlags().IntVarP(&downloadFlags.fetchSize, "parallel", "p", downloadFlags.fetchSize, "download objects concurrently")
	rootCmd.AddCommand(downloadCmd)
}

type downloadingItemVisitor struct {
	force  bool
	bucket s3sss.S3Bucket
	key    string
	path   string
}

func (div downloadingItemVisitor) VisitListing(partialResult *s3sss.S3ListBucketResult) bool {
	for _, item := range partialResult.Contents {
		tmpPath := div.path
		if tmpPath == "" {
			tmpPath = "."
		}
		if !strings.HasSuffix(tmpPath, "/") {
			tmpPath = tmpPath + "/"
		}
		itemKey := strings.TrimPrefix(item.Key, "/")
		div.bucket.Download(item.Key, tmpPath+itemKey, div.force)
	}
	return true
}

func download(cmd *cobra.Command, args []string) {
	bucket := s3sss.FindBucket(args[0])
	key := args[1]
	path, err := os.Getwd()
	s3base.CheckIfError(2, err)
	if len(args) > 2 {
		path = args[2]
	}

	if downloadFlags.recursive {
		bucket.List(key, downloadFlags.fetchSize, downloadingItemVisitor{
			force:  downloadFlags.force,
			bucket: bucket,
			key:    key,
			path:   path,
		})
	} else {
		if path == "" {
			bucket.Download(key, key, downloadFlags.force)

		} else {
			bucket.Download(key, path, downloadFlags.force)
		}
	}
}
