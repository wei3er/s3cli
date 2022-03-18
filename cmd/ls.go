package cmd

import (
	"fmt"
	s3base "s3cli/base"
	s3sss "s3cli/sss"
	"strconv"

	"github.com/spf13/cobra"
)

//######

type dumpingItemVisitor struct {
}

//######
var (
	lsFlags = struct {
		fetchSize      int
		humanReadable  bool
		longListFormat bool
	}{
		fetchSize:      1000,
		humanReadable:  false,
		longListFormat: false,
	}
	lsCmd = &cobra.Command{
		Use:        "ls [flags] <bucket> [prefix]",
		Aliases:    []string{"get", "list"},
		Short:      "list objects",
		Long:       `list information about objects in S3`,
		Run:        ls,
		Args:       cobra.MinimumNArgs(1),
		ArgAliases: []string{"bucket", "prefix"},
	}
)

func init() {
	lsCmd.PersistentFlags().IntVarP(&lsFlags.fetchSize, "fetch-size", "n", lsFlags.fetchSize, "fetch objects in batches of this size")
	lsCmd.PersistentFlags().BoolVarP(&lsFlags.longListFormat, "long-list", "l", lsFlags.longListFormat, "use a long listing format")
	lsCmd.PersistentFlags().BoolVarP(&lsFlags.humanReadable, "human-readable", "H", lsFlags.humanReadable, "print object sizes by the powers of 1024 instead of in bytes")
	rootCmd.AddCommand(lsCmd)
}

func ls(cmd *cobra.Command, args []string) {
	prefix := ""
	if len(args) > 1 {
		prefix = args[1]
	}
	bucket := s3sss.FindBucket(args[0])
	bucket.List(prefix, lsFlags.fetchSize, dumpingItemVisitor{})
}

func (div dumpingItemVisitor) VisitListing(partialResult *s3sss.S3ListBucketResult) bool {
	for _, item := range partialResult.Contents {
		row := item.Key
		if lsFlags.longListFormat {
			size := strconv.FormatInt(item.Size, 10)
			if lsFlags.humanReadable {
				size = s3base.ByteCountIEC(item.Size)
			}
			row = fmt.Sprintf("%s\t%s(%s)\t%s\t%s", item.StorageClass, item.Owner.DisplayName, item.Owner.Id, size, row)
		}
		fmt.Println(row)
	}
	return true
}
