package cmd

import (
	"fmt"
	"log"
	s3sss "s3cli/sss"

	cobra "github.com/spf13/cobra"
)

var (
	rmFlags = struct {
		fetchSize int
		recursive bool
	}{
		recursive: false,
		fetchSize: 1000,
	}
	rmCmd = &cobra.Command{
		Use:        "rm [flags] <bucket-name> [object-path]...",
		Aliases:    []string{"remove", "delete"},
		Short:      "remove objects",
		Long:       `remove objects by path from S3.`,
		Run:        rm,
		Args:       cobra.ExactArgs(2),
		ArgAliases: []string{"bucket", "key-prefix"},
	}
)

type deletingItemVisitor struct {
	failed bool
	keys   []string
}

func init() {
	rmCmd.PersistentFlags().BoolVarP(&rmFlags.recursive, "recursive", "r", rmFlags.recursive, "remove directories and their contents recursively")
	rmCmd.PersistentFlags().IntVarP(&rmFlags.fetchSize, "fetch-size", "n", rmFlags.fetchSize, "fetch objects in batches of this size")
	rootCmd.AddCommand(rmCmd)
}

func (div *deletingItemVisitor) VisitDeletion(partialResult *s3sss.S3DeleteResult) bool {
	for _, item := range partialResult.Deleted {
		fmt.Printf("'%s' deleted\n", item.Key)
	}
	for _, item := range partialResult.Error {
		div.failed = true
		fmt.Printf("'%s' not deleted: %s -> %s\n", item.Key, item.Code, item.Message)
	}
	return true
}

func (div *deletingItemVisitor) VisitListing(partialResult *s3sss.S3ListBucketResult) bool {
	for _, item := range partialResult.Contents {
		div.keys = append(div.keys, item.Key)
	}
	return true
}

func rm(cmd *cobra.Command, args []string) {
	bucket := s3sss.FindBucket(args[0])
	key := args[1]

	visitor := &deletingItemVisitor{
		keys:   make([]string, 0),
		failed: false,
	}
	bucket.List(key, rmFlags.fetchSize, visitor)
	if len(visitor.keys) == 0 {
		log.Fatalf("no objects found for key '%s'!", key)
	}
	if len(visitor.keys) > 1 && !rmFlags.recursive {
		log.Fatalf("%d objects found for key '%s'!\nretry using -r switch.", len(visitor.keys), key)
	}
	bucket.Delete(visitor, visitor.keys...)
	if visitor.failed {
		log.Fatal("at least one object was not deleted!")
	}
}
