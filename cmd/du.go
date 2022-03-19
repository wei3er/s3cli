package cmd

import (
	"fmt"
	s3base "s3cli/base"
	s3 "s3cli/s3"

	"github.com/spf13/cobra"
)

//######

type usageComputingItemVisitor struct {
	Count int
	Size  int64
}

//######
var (
	duFlags = struct {
		fetchSize      int
		humanReadable  bool
		longListFormat bool
	}{
		fetchSize:      1000,
		humanReadable:  false,
		longListFormat: false,
	}
	duCmd = &cobra.Command{
		Use:        "du [flags] <bucket> [prefix]",
		Aliases:    []string{"usage", "disk-usage"},
		Short:      "disk usage of objects",
		Long:       `shows disk usage of objects in S3`,
		Run:        du,
		Args:       cobra.MinimumNArgs(1),
		ArgAliases: []string{"bucket", "prefix"},
	}
)

func init() {
	duCmd.PersistentFlags().IntVarP(&duFlags.fetchSize, "fetch-size", "n", duFlags.fetchSize, "fetch objects in batches of this size")
	duCmd.PersistentFlags().BoolVarP(&duFlags.humanReadable, "human-readable", "H", duFlags.humanReadable, "print object sizes by the powers of 1024 instead of in bytes")
	rootCmd.AddCommand(duCmd)
}

func du(cmd *cobra.Command, args []string) {
	prefix := ""
	if len(args) > 1 {
		prefix = args[1]
	}
	bucket := FindBucket(args[0])
	visitor := &usageComputingItemVisitor{}
	bucket.List(prefix, duFlags.fetchSize, visitor)
	if duFlags.humanReadable {
		fmt.Println(visitor.Count, "object(s) using", s3base.ByteCountIEC(visitor.Size))
	} else {
		fmt.Println(visitor.Count, "object(s) using", visitor.Size, "B")
	}
}

func (uciv *usageComputingItemVisitor) VisitListing(partialResult *s3.S3ListBucketResult) (bool, error) {
	for _, item := range partialResult.Contents {
		uciv.Count = uciv.Count + 1
		uciv.Size = uciv.Size + item.Size
	}
	return true, nil
}
