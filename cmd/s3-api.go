package cmd

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/lock"
	"github.com/minio/minio/pkg/mimedb"
	"io"
	"net/http"
	"os"
	pathutil "path"
	"strings"
	"time"
)

const (
	maxPartSize = int64(5 * 1024 * 1024)
	maxRetries  = 3
)

type S3Api struct {
	*FSObjects
}

func (s *S3Api) initSvc() *s3.S3 {
	creds := credentials.NewStaticCredentials(GlobalS3Conf.AccessKeyID, GlobalS3Conf.SecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
		return nil
	}
	cfg := aws.NewConfig().WithEndpoint(GlobalS3Conf.BucketEndPoint).WithRegion(GlobalS3Conf.Region).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)
	return svc
}

func (s *S3Api) MultipartUpload(bucket string, key string, filepath string, ) (*s3.CompleteMultipartUploadOutput, error) {

	creds := credentials.NewStaticCredentials(GlobalS3Conf.AccessKeyID, GlobalS3Conf.SecretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		fmt.Printf("bad credentials: %s", err)
	}
	cfg := aws.NewConfig().WithEndpoint(GlobalS3Conf.BucketEndPoint).WithRegion(GlobalS3Conf.Region).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)
	file, err := os.Open(key)
	if err != nil {
		fmt.Printf("err opening file: %s", err)
		return nil, err
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	file.Read(buffer)
	input := &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(fileType),
	}

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	fmt.Println("Created multipart upload request")

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1
	for curr = 0; remaining != 0; curr += partLength {
		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}
		completedPart, err := s.S3uploadPart(svc, resp, buffer[curr:curr+partLength], partNumber)
		if err != nil {
			fmt.Println(err.Error())
			err := s.S3abortMultipartUpload(svc, resp)
			if err != nil {
				fmt.Println(err.Error())
				return nil, err
			}
		}
		remaining -= partLength
		partNumber++
		completedParts = append(completedParts, completedPart)
	}

	completeResponse, err := s.S3completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}
	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
	return completeResponse, nil
}

func (s *S3Api) S3Upload(svc *s3.S3, bucket string, p io.Reader, key string) (*s3.PutObjectOutput, error) {
	s3bucket := getS3BucketByConfig(bucket)
	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(p),
		Bucket: aws.String(s3bucket),
		Key:    aws.String(key),
		//Tagging:              aws.String("key1=value1&key2=value2"),
	}
	fmt.Println(s3bucket)
	fmt.Println(key)

	fmt.Println(aws.String(mimedb.TypeByExtension(pathutil.Ext(key))))
	//file, err := os.OpenFile("/Users/shenyang/mingdao/test.dmg", os.O_WRONLY|os.O_APPEND, 0666)
	//body := io.TeeReader(r.Body, h)
	result, err := svc.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return nil, err
	}
	fmt.Println(result)
	return result, nil
}

func (s *S3Api) S3completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func (s *S3Api) S3uploadPart(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, fileBytes []byte, partNumber int) (*s3.CompletedPart, error) {
	tryNum := 1
	partInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(fileBytes),
		Bucket:        resp.Bucket,
		Key:           resp.Key,
		PartNumber:    aws.Int64(int64(partNumber)),
		UploadId:      resp.UploadId,
		ContentLength: aws.Int64(int64(len(fileBytes))),
	}

	for tryNum <= maxRetries {
		uploadResult, err := svc.UploadPart(partInput)
		if err != nil {
			if tryNum == maxRetries {
				if aerr, ok := err.(awserr.Error); ok {
					return nil, aerr
				}
				return nil, err
			}
			fmt.Printf("Retrying to upload part #%v\n", partNumber)
			tryNum++
		} else {
			fmt.Printf("Uploaded part #%v\n", partNumber)
			return &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(partNumber)),
			}, nil
		}
	}
	return nil, nil
}

func (s *S3Api) S3Copytest(svc *s3.S3) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String("oss-mdtest2"),
		CopySource: aws.String("/oss-mdtest/ideaIU-2020.2.dmg"),
		Key:        aws.String("ideaIU-2020.3.dmg"),
	}

	_, err := svc.CopyObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectNotInActiveTierError:
				fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
				return aerr
			default:
				fmt.Println(aerr.Error())
				return err
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
			return err
		}
	}
	return nil
}

func (s *S3Api) S3abortMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput) error {
	fmt.Println("Aborting multipart upload for UploadId#" + *resp.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := svc.AbortMultipartUpload(abortInput)
	return err
}

func (s *S3Api) S3PutObject(ctx context.Context, bucket string, object string, r *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, retErr error) {
	var err error
	fs := s
	//data := r.Reader
	fsMeta := newFSMetaV1()
	fsMeta.Meta = map[string]string{}
	// Validate if bucket name is valid and exists.
	//if _, err = fs.statBucketDir(ctx, bucket); err != nil {
	//	return ObjectInfo{}, toObjectErr(err, bucket)
	//}
	var wlk *lock.LockedFile

	//svc := s.initSvc()

	//var tmp bytes.Buffer
	//io.TeeReader(r, &tmp)

	//result, err := s.S3Upload(svc, bucket, tee, object)

	//var buf bytes.Buffer
	//tee := io.TeeReader(r, &buf)

	//ioutil.ReadAll(&buf)
	if err != nil {
		logger.LogIf(ctx, err)
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}

	if bucket != minioMetaBucket {
		bucketMetaDir := pathJoin(fs.fsPath, minioMetaBucket, bucketMetaPrefix)
		fsMetaPath := pathJoin(bucketMetaDir, bucket, object, fs.metaJSONFile)
		wlk, err = fs.rwPool.Create(fsMetaPath)
		if err != nil {
			logger.LogIf(ctx, err)
			return ObjectInfo{}, toObjectErr(err, bucket, object)
		}
		//This close will allow for locks to be synchronized on `fs.json`.
		defer wlk.Close()
		defer func() {
			//Remove meta file when PutObject encounters any error
			if retErr != nil {
				tmpDir := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID)
				fsRemoveMeta(ctx, bucketMetaDir, fsMetaPath, tmpDir)
			}
		}()
	}

	data := r.Reader
	bufSize := int64(readSizeV1)
	if size := data.Size(); size > 0 && bufSize > size {
		bufSize = size
	}

	buf := make([]byte, int(bufSize))
	tempObj := mustGetUUID()
	fsTmpObjPath := pathJoin(fs.fsPath, minioMetaTmpBucket, fs.fsUUID, tempObj)

	bytesWritten, err := fsCreateFile(ctx, fsTmpObjPath, data, buf, data.Size())

	//fsMeta.Meta["etag"] = *result.ETag
	s3file, _ := os.Open(fsTmpObjPath)
	svc := s.initSvc()
	result, err := s.S3Upload(svc, bucket, s3file, object)

	defer s3file.Close()

	if bytesWritten < data.Size() {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, IncompleteBody{}
	}

	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	fsMeta.Meta["etag"] = trimQuotes(*result.ETag)

	defer fsRemoveFile(ctx, fsTmpObjPath)

	if bytesWritten < data.Size() {
		fsRemoveFile(ctx, fsTmpObjPath)
		return ObjectInfo{}, IncompleteBody{}
	}

	//fsNSObjPath := pathJoin(fs.fsPath, bucket, object)
	//if err = fsRenameFile(ctx, fsTmpObjPath, fsNSObjPath); err != nil {
	//	return ObjectInfo{}, toObjectErr(err, bucket, object)
	//}

	//if bucket != minioMetaBucket {
	// Write FS metadata after a successful namespace operation.
	//记录fs文件是必须的
	//if _, err = fsMeta.WriteTo(wlk); err != nil {
	//	return ObjectInfo{}, toObjectErr(err, bucket, object)
	//}
	//}
	fi, err := fsStatFile(ctx, fsTmpObjPath)
	if err != nil {
		return ObjectInfo{}, toObjectErr(err, bucket, object)
	}
	return fsMeta.ToObjectInfo(bucket, object, fi), nil

}

func (s *S3Api) S3checkObject(svc *s3.S3, bucket string, object string) error {
	s3bucket := getS3BucketByConfig(bucket)
	input := &s3.GetObjectAclInput{
		Bucket: aws.String(s3bucket),
		Key:    aws.String(object),
	}

	_, err := svc.GetObjectAcl(input)

	if err != nil {
		return err
	} else {
		return nil
	}
}

func (s *S3Api) S3getObject(svc *s3.S3, rs *HTTPRangeSpec, h http.Header, contentyppe string, bucket string, object string) (out *s3.GetObjectOutput, err error) {
	s3bucket := getS3BucketByConfig(bucket)

	var rangeHeader string
	if rs == nil {
		rangeHeader = ""
	} else {
		rangeHeader = h.Get("Range")
	}
	if strings.Contains(contentyppe, "text") {
		contentyppe = "text"
	}
	input := &s3.GetObjectInput{
		Bucket:              aws.String(s3bucket),
		Key:                 aws.String(object),
		ResponseContentType: aws.String(contentyppe),
		Range:               aws.String(rangeHeader),
	}

	//if rs!=nil{
	//
	//}

	output, err := svc.GetObject(input)

	if err != nil {
		return nil, err
	} else {
		return output, nil
	}
}

// GetObjectNInfo - returns object info and a reader for object
// content.

func (s *S3Api) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (oi ObjectInfo, err error) {
	var objInfo ObjectInfo
	svc := s.initSvc()
	if err = s.S3checkObject(svc, bucket, object); err != nil {
		return objInfo, err
	}

	//if _, err = s.statBucketDir(ctx, bucket); err != nil {
	//	return nil, toObjectErr(err, bucket)
	//}

	//m := fsMetaV1{}
	m := s.defaultFsJSON(object)

	if m.Meta["content-type"] == "" {
		m.Meta["content-type"] = mimedb.TypeByExtension(pathutil.Ext(object))
	}

	s3File, err := s.S3getObject(svc, nil, nil, m.Meta["content-type"], bucket, object)

	if err != nil {
		return objInfo, err
	}

	// Otherwise we get the object info

	objInfo.ETag = trimQuotes(*s3File.ETag)
	objInfo.Bucket = bucket
	objInfo.Name = object
	objInfo.ModTime = *s3File.LastModified
	objInfo.Size = *s3File.ContentLength
	objInfo.ContentType = *s3File.ContentType
	if s3File.ContentEncoding != nil {
		objInfo.ContentEncoding = *s3File.ContentEncoding
	}

	if s3File.Expires != nil {
		expirestime, _ := time.ParseInLocation("2006-01-02", *s3File.Expires, time.Local)
		objInfo.Expires = expirestime
	}

	//objInfo.StorageClass = *s3File.StorageClass
	objInfo.UserDefined = cleanMetadata(m.Meta)

	return objInfo, nil

}
func (s *S3Api) S3GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lockType LockType, opts ObjectOptions, isCache int) (gr *GetObjectReader, err error) {
	svc := s.initSvc()
	if err = s.S3checkObject(svc, bucket, object); err != nil {
		return nil, err
	}

	//if _, err = s.statBucketDir(ctx, bucket); err != nil {
	//	return nil, toObjectErr(err, bucket)
	//}

	var nsUnlocker = func() {}

	if lockType != noLock {
		// Lock the object before reading.
		lock := s.nsMutex.NewNSLock(ctx, bucket, object)
		switch lockType {
		case writeLock:
			if err = lock.GetLock(globalObjectTimeout); err != nil {
				logger.LogIf(ctx, err)
				return nil, err
			}
			nsUnlocker = lock.Unlock
		case readLock:
			if err = lock.GetRLock(globalObjectTimeout); err != nil {
				logger.LogIf(ctx, err)
				return nil, err
			}
			nsUnlocker = lock.RUnlock
		}
	}

	//m := fsMetaV1{}
	m := s.defaultFsJSON(object)

	if m.Meta["content-type"] == "" {
		m.Meta["content-type"] = mimedb.TypeByExtension(pathutil.Ext(object))
	}

	var s3File *s3.GetObjectOutput
	if isCache != 2 {
		s3File, err = s.S3getObject(svc, rs, h, m.Meta["content-type"], bucket, object)
		if err != nil {
			return nil, err
		}
	}

	// Otherwise we get the object info
	var objInfo ObjectInfo

	objInfo.Bucket = bucket
	objInfo.Name = object

	if isCache != 2 {
		objInfo.ETag = trimQuotes(*s3File.ETag)
		objInfo.ModTime = *s3File.LastModified
		objInfo.Size = *s3File.ContentLength
		objInfo.ContentType = *s3File.ContentType
		if s3File.ContentEncoding != nil {
			objInfo.ContentEncoding = *s3File.ContentEncoding
		}
		if s3File.Expires != nil {
			expirestime, _ := time.ParseInLocation("2006-01-02", *s3File.Expires, time.Local)
			objInfo.Expires = expirestime
		}
	}



	//objInfo.StorageClass = *s3File.StorageClass
	objInfo.UserDefined = cleanMetadata(m.Meta)

	fsObjPath := pathJoin(globalTmpPrefix, objInfo.ETag)
	rwPoolUnlocker := func() {}

	if isCache == 1 {
		localFile, err := os.Create(fsObjPath)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		if _, err = io.Copy(localFile, s3File.Body); err != nil {
			fmt.Println(err)
			return nil, err
		}

		objInfo.path = fsObjPath

		objReaderFn, off, length, rErr := NewGetObjectReader(rs, objInfo, opts.CheckCopyPrecondFn, nsUnlocker, rwPoolUnlocker)
		if rErr != nil {
			return nil, rErr
		}

		readCloser, size, err := fsOpenFile(ctx, fsObjPath, off)
		if err != nil {
			rwPoolUnlocker()
			nsUnlocker()
			return nil, toObjectErr(err, bucket, object)
		}
		reader := io.LimitReader(readCloser, length)
		closeFn := func() {
			readCloser.Close()
		}

		if off > size || off+length > size {
			err = InvalidRange{off, length, size}
			logger.LogIf(ctx, err)
			closeFn()
			rwPoolUnlocker()
			nsUnlocker()
			return nil, err
		}
		return objReaderFn(reader, h, opts.CheckCopyPrecondFn, closeFn)
	} else {
		objReaderFn, _, _, rErr := NewGetObjectReader(rs, objInfo, opts.CheckCopyPrecondFn, nsUnlocker, rwPoolUnlocker)
		if rErr != nil {
			return nil, rErr
		}
		closeFn := func() {
			if s3File!=nil{
				s3File.Body.Close()
			}
		}
		if isCache == 2{
			return objReaderFn(nil, h, opts.CheckCopyPrecondFn, closeFn)
		}else{
			return objReaderFn(s3File.Body, h, opts.CheckCopyPrecondFn, closeFn)
		}
	}
}

func (s *S3Api) S3copyObject(ctx context.Context, srcBucket string, srcObject string, dstBucket string, dstObject string) error {
	s3srcbucket := getS3BucketByConfig(srcBucket)
	s3dstbucket := getS3BucketByConfig(dstBucket)

	svc := s.initSvc()
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(s3dstbucket),
		CopySource: aws.String("/" + s3srcbucket + "/" + srcObject),
		Key:        aws.String(dstObject),
	}

	result, err := svc.CopyObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeObjectNotInActiveTierError:
				fmt.Println(s3.ErrCodeObjectNotInActiveTierError, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}

	}
	fmt.Println(result)
	return nil
}

func getS3BucketByConfig(bucket string) string {
	s3bucket, ok := GlobalS3Conf.BucketName[bucket]
	if ok {
		return s3bucket.(string)
	} else {
		return bucket
	}
}

func trimQuotes(s string) string {
	if len(s) >= 2 {
		if c := s[len(s)-1]; s[0] == c && (c == '"' || c == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
