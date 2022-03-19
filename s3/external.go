package s3

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type S3Bucket struct {
	Name        string
	Endpoint    string
	SecretKey   string
	AccessKeyId string
	Region      string
}

type S3Owner struct {
	Id          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type S3Item struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	Owner        S3Owner   `xml:"Owner"`
	StorageClass string    `xml:"StorageClass"`
}

type S3ListBucketResult struct {
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix"`
	Delimiter             string   `xml:"Delimiter"`
	MaxKeys               int      `xml:"MaxKeys"`
	EncodingType          string   `xml:"EncodingType"`
	KeyCount              int      `xml:"KeyCount"`
	ContinuationToken     string   `xml:"ContinuationToken"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	StartAfter            string   `xml:"StartAfter"`
	Contents              []S3Item `xml:"Contents"`
}

type S3ListBucketResultVisitor interface {
	VisitListing(partialResult *S3ListBucketResult) (bool, error)
}

//#######

type S3Deleted struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type S3DeleteResult struct {
	Deleted []S3Deleted `xml:"Deleted"`
	Error   []S3Deleted `xml:"Error"`
}

type S3DeleteResultVisitor interface {
	VisitDeletion(partialResult *S3DeleteResult) error
}

//#######

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (bucket S3Bucket) List(prefix string, fetchSize int, visitor S3ListBucketResultVisitor) error {
	query := "list-type=2&fetch-owner=true&max-keys=" + strconv.Itoa(fetchSize)
	if prefix != "" {
		query += "&prefix=" + url.QueryEscape(prefix)
	}

	var rlt S3ListBucketResult
	reqUrl, err := url.Parse(bucket.Endpoint + "/?" + query)
	if err != nil {
		return err
	}
	for {

		resp, err := curl(bucket, "GET", reqUrl, http.NoBody)
		if err != nil {
			return err
		}
		xml.Unmarshal(resp.Bytes(), &rlt)
		b, err := visitor.VisitListing(&rlt)
		if err != nil {
			return err
		}
		if !b {
			return nil
		}
		if rlt.KeyCount != rlt.MaxKeys {
			return nil
		}
		tmp := reqUrl.Query()
		tmp.Set("start-after", rlt.Contents[len(rlt.Contents)-1].Key)
		reqUrl.RawQuery = tmp.Encode()
	}
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (bucket S3Bucket) Download(key string, targetPath string) error {
	reqUrl, err := url.Parse(bucket.Endpoint + "/" + key)
	if err != nil {
		return err
	}
	resp, err := curl(bucket, "GET", reqUrl, http.NoBody)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(targetPath, resp.Bytes(), fs.ModePerm)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
func (bucket S3Bucket) Delete(visitor S3DeleteResultVisitor, keys ...string) error {
	var payload string
	payload = "<Delete>"
	for _, key := range keys {
		payload += fmt.Sprintf("<Object><Key>%s</Key></Object>", key)
	}
	payload += "</Delete>"

	reqUrl, err := url.Parse(bucket.Endpoint + "/?delete")
	if err != nil {
		return err
	}
	resp, err := curl(bucket, "POST", reqUrl, strings.NewReader(payload))
	if err != nil {
		return err
	}
	var rlt S3DeleteResult
	xml.Unmarshal(resp.Bytes(), &rlt)
	return visitor.VisitDeletion(&rlt)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (bucket S3Bucket) Upload(key string, file *os.File) error {
	filepath := file.Name()
	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("can not read file at %s: %s", filepath, err.Error())
	}
	if fi.IsDir() {
		return fmt.Errorf("file %s is a directory", filepath)
	}

	reqUrl, err := url.Parse(bucket.Endpoint + "/" + key)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	_, err = curl(bucket, "PUT", reqUrl, bytes.NewReader(b))
	return err
}
