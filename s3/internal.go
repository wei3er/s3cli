package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func curl(bucket S3Bucket, method string, reqUrl *url.URL, payload io.Reader) (*bytes.Buffer, error) {
	/*
	 * perform http request
	 *
	 */
	req, err := http.NewRequest(method, reqUrl.String(), payload)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: time.Second * time.Duration(10),
	}
	err = signAwsV4(bucket, req)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	err = nil
	if resp.StatusCode != 200 {
		err = errors.New("http response was: " + strconv.Itoa(resp.StatusCode) + " / " + buf.String())
	}
	return buf, err
}

/*
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
 * https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
 *
 */
func signAwsV4(b S3Bucket, req *http.Request) error {
	now := time.Now().UTC()

	header := make(map[string]string)
	header["X-Amz-Date"] = now.Format("20060102T150405Z")
	header["User-Agent"] = "s3 cli"
	header["Accept"] = "*/*"
	header["Accept-Encoding"] = "gzip, deflate, br"
	header["Connection"] = "keep-alive"

	/*
	 * read payload content
	 *
	 */
	var content []byte
	if req.GetBody != nil {
		payload, err := req.GetBody()
		if err != nil {
			return err
		}
		content, err = ioutil.ReadAll(payload)
		if err != nil {
			return err
		}
	}

	hash := sha256.New()
	hash.Write(content)
	contentHash := fmt.Sprintf("%x", hash.Sum(nil))
	header["X-Amz-Content-Sha256"] = contentHash

	/*
	 * compute signing content
	 *
	 */
	// HTTPMethod
	canonicalRequest := strings.ToUpper(req.Method)
	// CanonicalURI
	canonicalRequest += "\n" + strings.ReplaceAll(req.URL.Path, "=", "%3D")
	// CanonicalQueryString
	query := req.URL.Query()
	if len(query) == 0 {
		canonicalRequest += "\n"
	} else {
		canonicalRequest += "\n" + query.Encode()
	}
	// CanonicalHeaders
	signedHeaders := "host"
	canonicalRequest += "\n" + "host:" + req.URL.Host
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

	signingContent := "AWS4-HMAC-SHA256"
	signingContent += "\n" + now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/" + b.Region + "/s3/aws4_request"
	signingContent += "\n" + scope
	hash = sha256.New()
	hash.Write([]byte(canonicalRequest))
	signingContent += "\n" + fmt.Sprintf("%x", hash.Sum(nil))

	/*
	 * compute signing key
	 *
	 */
	mac := hmac.New(sha256.New, []byte("AWS4"+b.SecretKey))
	mac.Write([]byte(now.Format("20060102")))
	dateKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateKey)
	mac.Write([]byte(b.Region))
	dateRegionKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateRegionKey)
	mac.Write([]byte("s3"))
	dateRegionServiceKey := mac.Sum(nil)

	mac = hmac.New(sha256.New, dateRegionServiceKey)
	mac.Write([]byte("aws4_request"))
	signingKey := mac.Sum(nil)

	/*
	 * create signature
	 *
	 */
	mac = hmac.New(sha256.New, []byte(signingKey))
	mac.Write([]byte(signingContent))
	signature := fmt.Sprintf("%x", mac.Sum(nil))
	header["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + b.AccessKeyId + "/" + scope + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature

	/*
	 * create MD5 checksum of content
	 *
	 */
	hash = md5.New()
	hash.Write([]byte(content))
	header["Content-MD5"] = base64.StdEncoding.EncodeToString(hash.Sum(nil))

	// write header to request
	for k, v := range header {
		req.Header.Set(k, v)
	}
	return nil
}
