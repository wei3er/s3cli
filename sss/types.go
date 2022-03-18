package sss

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
	Key          string  `xml:"Key"`
	LastModified string  `xml:"LastModified"`
	ETag         string  `xml:"ETag"`
	Size         int64   `xml:"Size"`
	Owner        S3Owner `xml:"Owner"`
	StorageClass string  `xml:"StorageClass"`
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
	VisitListing(partialResult *S3ListBucketResult) bool
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
	VisitDeletion(partialResult *S3DeleteResult) bool
}
