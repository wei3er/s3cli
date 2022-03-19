package cmd

import (
	"os"
	"path"
	s3base "s3cli/base"
	s3 "s3cli/s3"
	"strings"

	log "github.com/sirupsen/logrus"

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
		Run:        down,
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
	bucket s3.S3Bucket
	path   string
}

func (div downloadingItemVisitor) VisitListing(partialResult *s3.S3ListBucketResult) (bool, error) {
	for _, item := range partialResult.Contents {
		download(div.bucket, item.Key, div.path, false)
	}
	return true, nil
}

func down(cmd *cobra.Command, args []string) {
	bucket := FindBucket(args[0])
	key := args[1]

	path, err := os.Getwd()
	s3base.CheckIfError(2, err)
	if len(args) > 2 {
		path = args[2]
	}

	fi, err := os.Stat(path)
	if downloadFlags.recursive {
		if err != nil {
			if !os.IsNotExist(err) {
				log.Fatalf("error reading stats of %s: %w", path, err)
			}
			bucket.List(key, downloadFlags.fetchSize, downloadingItemVisitor{
				bucket: bucket,
				path:   path,
			})
			return

		}
		if fi.IsDir() {
			bucket.List(key, downloadFlags.fetchSize, downloadingItemVisitor{
				bucket: bucket,
				path:   path,
			})
			return
		}
		log.Fatalf("recursive download target %s needs to be a directory", path)
		return
	}
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatalf("error reading stats of %s: %w", path, err)
			return
		}
		download(bucket, key, path, true)
		return

	} else {
		if fi.IsDir() {
			download(bucket, key, path, false)
			return
		}
		if downloadFlags.force {
			download(bucket, key, path, true)
			return
		}
		log.Fatalf("file %s already exists (use force flag)", path)
		return
	}
}

func download(bucket s3.S3Bucket, key string, targetPath string, exact bool) {
	target := targetPath
	if !exact {
		target = convertKey(targetPath, key)
	}
	_, err := os.Stat(target)
	if err == nil {
		if !downloadFlags.force {
			log.Fatalf("file %s already exists (use force flag)", target)
		}
	} else {
		if !os.IsNotExist(err) {
			log.Fatal(err)
		}
	}
	dir := path.Dir(target)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	err = bucket.Download(key, target)
	if err != nil {
		log.Fatal(err)
	}
}

func convertKey(path string, key string) string {
	sep := string(os.PathSeparator)
	return strings.TrimSuffix(path, sep) + sep + strings.TrimPrefix(strings.ReplaceAll(key, downloadFlags.keyToPathDelimiter, sep), sep)
}
