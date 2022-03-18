package sss

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	s3base "s3cli/base"

	log "github.com/sirupsen/logrus"
	viper "github.com/spf13/viper"
)

func FindBucket(name string) S3Bucket {
	var s3Buckets []S3Bucket
	viper.UnmarshalKey("buckets", &s3Buckets)
	for _, b := range s3Buckets {
		if name == b.Name {
			return b
		}
	}
	log.Fatal("unknown bucket specified: ", name)
	return S3Bucket{}
}

/*
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
 *
 */
func curl(bucket S3Bucket, method string, path string, header map[string]string, query map[string]string, timeouts int64, payload io.Reader, addContentMd5 bool) (*bytes.Buffer, int) {
	now := time.Now().UTC()
	if header == nil {
		header = make(map[string]string)
	}
	header["X-Amz-Date"] = now.Format("20060102T150405Z")
	header["User-Agent"] = "s3 cli"
	header["Accept"] = "*/*"
	header["Accept-Encoding"] = "gzip, deflate, br"
	header["Connection"] = "keep-alive"

	/*
	 * read payload content
	 *
	 */
	content, err := ioutil.ReadAll(payload)
	log.Debugf("content is:\n%s", string(content))
	s3base.CheckIfError(3, err)
	hash := sha256.New()
	hash.Write(content)
	contentHash := fmt.Sprintf("%x", hash.Sum(nil))
	header["X-Amz-Content-Sha256"] = contentHash

	/*
	 * compute signing content
	 *
	 */
	u, err := url.Parse(bucket.Endpoint + path)
	s3base.CheckIfError(4, err)
	if len(query) > 0 {
		values := u.Query()
		for k, v := range query {
			values.Set(k, v)
		}
		u.RawQuery = values.Encode()
	}
	// HTTPMethod
	canonicalRequest := strings.ToUpper(method)
	// CanonicalURI
	canonicalRequest += "\n" + strings.ReplaceAll(u.Path, "=", "%3D")
	// CanonicalQueryString
	if len(query) == 0 {
		canonicalRequest += "\n"
	} else {
		canonicalRequest += "\n" + u.Query().Encode()
	}
	// CanonicalHeaders
	signedHeaders := "host"
	canonicalRequest += "\n" + "host:" + u.Host
	keys := make([]string, 0, len(header))
	for k := range header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kl := strings.ToLower(k)
		if strings.HasPrefix(kl, "x-amz-") {
			signedHeaders += ";" + kl
			canonicalRequest += "\n" + kl + ":" + header[k]
		}
	}
	canonicalRequest += "\n"
	// SignedHeaders
	canonicalRequest += "\n" + signedHeaders
	// HashedPayload
	canonicalRequest += "\n" + contentHash
	log.Debugf("canonical request:\n%s", canonicalRequest)

	signingContent := "AWS4-HMAC-SHA256"
	signingContent += "\n" + now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + bucket.Region + "/s3/aws4_request"
	signingContent += "\n" + scope
	hash = sha256.New()
	hash.Write([]byte(canonicalRequest))
	signingContent += "\n" + fmt.Sprintf("%x", hash.Sum(nil))
	log.Debugf("content to signed:\n%s", signingContent)

	/*
	 * compute signing key
	 *
	 */
	mac := hmac.New(sha256.New, []byte("AWS4"+bucket.SecretKey))
	mac.Write([]byte(now.Format("20060102")))
	dateKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateKey)
	mac.Write([]byte(bucket.Region))
	dateRegionKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateRegionKey)
	mac.Write([]byte("s3"))
	dateRegionServiceKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateRegionServiceKey)
	mac.Write([]byte("aws4_request"))
	signingKey := mac.Sum(nil)
	log.Debugf("signing key: %x", signingKey)

	/*
	 * create signature
	 *
	 */
	mac = hmac.New(sha256.New, []byte(signingKey))
	mac.Write([]byte(signingContent))
	signature := fmt.Sprintf("%x", mac.Sum(nil))
	log.Debugf("signature: %s", signature)
	header["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + bucket.AccessKeyId + "/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature

	/*
	 * perform http request
	 *
	 */
	log.Debug(method+" request to ", u.String())
	req, err := http.NewRequest(method, u.String(), bytes.NewReader(content))
	s3base.CheckIfError(8, err)
	if addContentMd5 {
		hash := md5.New()
		hash.Write([]byte(content))
		header["Content-MD5"] = base64.StdEncoding.EncodeToString(hash.Sum(nil))
	}
	for k, v := range header {
		log.Debugf("header %s: %s", k, v)
		req.Header.Add(k, v)
	}

	var resp *http.Response
	client := &http.Client{
		Timeout: time.Second * time.Duration(timeouts),
	}
	resp, err = client.Do(req)
	s3base.CheckIfError(9, err)

	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return buf, resp.StatusCode
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func (bucket S3Bucket) List(prefix string, fetchSize int, visitor S3ListBucketResultVisitor) {
	log.Infof("listing items with prefix %s", prefix)

	query := make(map[string]string)
	query["list-type"] = "2"
	query["max-keys"] = strconv.Itoa(fetchSize)
	if prefix != "" {
		query["prefix"] = prefix
	}

	var rlt S3ListBucketResult
	for {
		resp, httpCode := curl(bucket, "GET", "", nil, query, 10, http.NoBody, false)
		if httpCode != 200 {
			log.Error("http response: ", resp.String())
			log.Fatal("http code was: ", httpCode)
		}
		xml.Unmarshal(resp.Bytes(), &rlt)
		if !visitor.VisitListing(&rlt) {
			return
		}
		if rlt.KeyCount == rlt.MaxKeys {
			query["start-after"] = rlt.Contents[len(rlt.Contents)-1].Key
		} else {
			return
		}
	}
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
func (bucket S3Bucket) Download(key string, filePath string, force bool) {
	log.Infof("downloading key %s to %s", key, filePath)

	fi, err := os.Stat(filePath)
	if err == nil {
		if !force {
			log.Fatal("destination ", filePath, " already exists")
		}
		if fi.IsDir() {
			log.Fatal("destination ", filePath, " is a directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Fatal("error reading ", filePath, ":", err.Error())
	} else {
		dir := path.Dir(filePath)
		err = os.MkdirAll(dir, os.ModePerm)
		s3base.CheckIfError(2, err)
	}

	query := make(map[string]string)
	resp, httpCode := curl(bucket, "GET", "/"+key, nil, query, 10, http.NoBody, false)
	if httpCode != 200 {
		log.Error("http response: ", resp.String())
		log.Fatal("http code was: ", httpCode)
	}
	err = ioutil.WriteFile(filePath, resp.Bytes(), fs.ModePerm)
	s3base.CheckIfError(4, err)
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
func (bucket S3Bucket) Delete(visitor S3DeleteResultVisitor, keys ...string) {
	log.Infof("deleting keys: %s", keys)

	var payload string
	payload = "<Delete>"
	for _, key := range keys {
		payload += fmt.Sprintf("<Object><Key>%s</Key></Object>", key)
	}
	payload += "</Delete>"

	query := make(map[string]string)
	query["delete"] = ""
	resp, httpCode := curl(bucket, "POST", "/", nil, query, 10, strings.NewReader(payload), true)
	if httpCode != 200 {
		log.Error("http response: ", resp.String())
		log.Fatal("http code was: ", httpCode)
	}
	var rlt S3DeleteResult
	xml.Unmarshal(resp.Bytes(), &rlt)
	if !visitor.VisitDeletion(&rlt) {
		return
	}
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func (bucket S3Bucket) Upload(key string, file *os.File) {

	filepath := file.Name()
	log.Infof("uploading object %s with key %s", filepath, key)
	fi, err := file.Stat()
	if err != nil {
		log.Fatalf("can not read file at %s: %s", filepath, err.Error())
	}
	if fi.IsDir() {
		log.Fatalf("file %s is a directory!", filepath)
	}

	query := make(map[string]string)
	resp, httpCode := curl(bucket, "PUT", "/"+key, nil, query, 10, file, true)
	if httpCode != 200 {
		log.Error("http response: ", resp.String())
		log.Fatal("http code was: ", httpCode)
	}
	s3base.CheckIfError(4, err)

}
