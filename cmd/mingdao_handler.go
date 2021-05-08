package cmd

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cavaliercoder/grab"
	"github.com/gorilla/mux"
	"github.com/minio/minio/pkg/hash"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
	pkgUtil "github.com/minio/minio/pkg/ioutil"

)

type qiniuRequestArg struct {
	key              string
	etag             string
	bucket           string
	serverName       string
	filePath         string
	fileName         string
	originalFileName string
	fileExt          string
	fileSize         int
	size             int
	fsize            string
	name             string
	fname            string
	tmpPath          string
	ext              string
}

func (q *qiniuRequestArg) String() string {
	return fmt.Sprintf("{fileName:%d, name:%d, orName:%d}", q.fileName, q.name,q.originalFileName)
}
type MutilPartResponse struct {
	Checksum string `json:"checksum"`
	Crc32    int    `json:"crc32"`
	Ctx      string `json:"ctx"`
	Expire   int    `json:"expired_at"`
	Host     string `json:"host"`
	Offset   int64  `json:"offset"`
}

type CompleteMutilResponse struct {
	Bucket string `json:"bucket"`
	Etag   string `json:"etag"`
	//FileExt string `json:"fileExt"`
	FileName string `json:"fileName"`
	Fsize    int    `json:"fsize"`
	Key      string `json:"key"`
}

type StatsResponse struct {
	Fsize    int64  `json:"fsize"`
	Hash     string `json:"hash"`
	MimeType string `json:"mimeType"`
	Type     int    `json:"type"`
	PutTime  int64  `json:"putTime"`
}

type FetchResponse struct {
	Fsize    int64  `json:"fsize"`
	Hash     string `json:"hash"`
	MimeType string `json:"mimeType"`
	Key      string `json:"key"`
}

type ErrorCode struct {
	code uint32
	msg  string
}

type ErrorMessage struct {
	Error string `json:"error"`
}

func (e *ErrorCode) Error() string {
	return fmt.Sprintf("code = %d ; msg = %s", e.code, e.msg)
}

type qiniuRequest struct {
	ReturnBody string `json:"returnBody"`
	Scope      string `json:"scope"`
	Deadline   int64  `json:"deadline"`
}

type qiniuBody struct {
	ReturnBody map[string]interface{} `json:"returnBody"`
	Scope      string                 `json:"scope"`
	Deadline   int64                  `json:"deadline"`
}

var mutex sync.Mutex


var blockExt = []string{".exe", ".vbs", ".bat", ".cmd", ".com", ".sh"}

func (web *webAPIHandlers) Test(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	//b := vars["EncodedEntryURI"]

	fmt.Print(vars)
	w.Write([]byte("HelloWorld"))
}

func (web *webAPIHandlers) BatchDownload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	cacheID, _ := vars["cacheID"]
	objInfo, _ := getDownloadFilesByCacheId(cacheID)
	//if mingimageformat.format!="" {
	//	w.Header().Set("Content-Type", objInfo.ContentType)
	//}
	statusCodeWritten := false
	reader, err := os.Open(objInfo.path)
	defer reader.Close()
	if err!=nil {
		 http.Error(w, "zipfile error", 400)

		return
	}
	//这里由于兼容format格式所以直接在这里重新定义
	if err := setObjectHeaders(w, objInfo, nil); err != nil {
		writeErrorResponse(context.Background(), w, toAPIError(context.Background(), err), r.URL, guessIsBrowserReq(r))
		return
	}
	setHeadGetRespHeaders(w, r.URL.Query())

	w.Header().Set("Content-Disposition", "attachment")
	w.Header().Set("Content-Disposition","attachment; "+ "filename=\""+"批量下载.zip"+"\"; "+"filename*=utf-8' '"+url.QueryEscape("批量下载.zip"))

	httpWriter := pkgUtil.WriteOnClose(w)
	if _, err := io.Copy(httpWriter, reader); err != nil {
		if !httpWriter.HasWritten() && !statusCodeWritten { // write error response only if no data or headers has been written to client yet
			writeErrorResponse(context.Background(), w, toAPIError(context.Background(), err), r.URL, guessIsBrowserReq(r))
		}
		return
	}
	return


}

func (web *webAPIHandlers) Putb64(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	Authorization := r.Header.Get("Authorization")
	//ctx := newContext(r, w, "Putb64")
	//baseUrl := r.RequestURI
	s := strings.Split(Authorization, " ")

	token := s[1]

	returnBody, err := getMagicargs(token)
	if err != nil {
		http.Error(w, "token parse error", 400)
		return
	}

	bucket := returnBody.Scope

	if Authorization == "UpToken "+token {
		//some check

	} else {
		//http.Error(w,"authorization error",401)
		//return
	}

	//b := vars["EncodedEntryURI"]

	fmt.Print(bucket)
	fmt.Print(vars)
	filekey, err := base64.URLEncoding.DecodeString(vars["encodedKey"])
	key := string(filekey)
	data, err := ioutil.ReadAll(r.Body)

	dist, _ := base64.StdEncoding.DecodeString(string(data))
	//n,err:=io.ReadFull(r.Body,ctxData)
	tmpPath := globalTmpPrefix + "tmp/" + "tmp"
	tmpfile, err := os.Create(tmpPath)
	fssize, _ := tmpfile.Write(dist)
	tmpfile.Close()

	if vars["fsize"] != "-1" {
		int, _ := strconv.Atoi(vars["fsize"])
		fssize = int
	}

	//文件hash计算
	var pReader *PutObjReader
	file, _ := os.OpenFile(tmpPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 066)
	defer fsRemoveFile(context.Background(), tmpPath)

	var reader io.Reader = file

	hashReader, err := hash.NewReader(reader, int64(fssize), "", "", int64(fssize), globalCLIContext.StrictS3Compat)
	if err != nil {
		http.Error(w, "hash error", 400)
		return
	}

	var opts ObjectOptions
	opts, err = putOpts(context.Background(), r, bucket, key, nil)

	pReader = NewPutObjReader(hashReader, nil, nil)

	fileArg := qiniuRequestArg{fileName: "null", fileExt: "null", filePath: "null", originalFileName: "null", serverName: "null"}
	fileArg.tmpPath = tmpPath

	var objInfo ObjectInfo

	objInfo, err = globalMingdaoFs.PutObject(context.Background(), bucket, key, pReader, opts)

	if err != nil {
		fmt.Println(err)
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 400)
		return
	}
	response := CompleteMutilResponse{Bucket: bucket, Etag: objInfo.ETag, FileName: key, Fsize: fssize, Key: key}

	jsonResponse, err := json.Marshal(response)

	if err != nil {
		errMsg := ErrorMessage{Error: "putb64 error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 500)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonResponse)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
	//	w.Write([]byte("HelloWorld"))
}

func (web *webAPIHandlers) Copy(w http.ResponseWriter, r *http.Request) () {
	Authorization := r.Header.Get("Authorization")
	ctx := newContext(r, w, "CopyObject")

	baseUrl := r.RequestURI
	credential := globalServerConfig.GetCredential()
	signature := hmacSha1(baseUrl, credential.SecretKey)
	token := credential.AccessKey + ":" + signature
	if Authorization == "QBox "+token {
		//some check

	} else {
		//http.Error(w,"authorization error",401)
		//return
	}
	objectAPI := globalObjectAPI
	vars := mux.Vars(r)
	srcURL, _ := base64.URLEncoding.DecodeString(vars["EncodedEntryURISrc"])
	EntryURI, _ := base64.URLEncoding.DecodeString(vars["EncodedEntryURIDest"])
	//force:=vars["force"]
	srcObjectList := strings.Split(string(srcURL), ":")
	srcBucket := srcObjectList[0]
	srcObject := srcObjectList[1]
	objectList := strings.Split(string(EntryURI), ":")
	dstBucket := objectList[0]
	dstObject := objectList[1]

	if globalMingdaoFs.Mode == 3 {
		globalMingdaoFs.S3copyObject(ctx, srcBucket, srcObject, dstBucket, dstObject)
	} else {
		getObjectNInfo := objectAPI.GetObjectNInfo
		var lock = noLock

		var getOpts = ObjectOptions{}

		var rs *HTTPRangeSpec
		gr, err := getObjectNInfo(ctx, srcBucket, srcObject, rs, r.Header, lock, getOpts)

		defer gr.Close()

		srcInfo := gr.ObjInfo

		srcInfo.Reader, err = hash.NewReader(gr, gr.ObjInfo.Size, "", "", gr.ObjInfo.Size, globalCLIContext.StrictS3Compat)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		//rawReader := srcInfo.Reader
		pReader := NewPutObjReader(srcInfo.Reader, nil, nil)

		var srcOpts, dstOpts ObjectOptions
		srcOpts, err = copySrcOpts(ctx, r, srcBucket, srcObject)
		if err != nil {
			writeErrorResponse(ctx, w, toAPIError(ctx, err), r.URL, guessIsBrowserReq(r))
			return
		}

		dstOpts, err = copyDstOpts(ctx, r, dstBucket, dstObject, nil)
		srcInfo.PutObjReader = pReader
		//var objInfo ObjectInfo
		_, err = objectAPI.CopyObject(ctx, srcBucket, srcObject, dstBucket, dstObject, srcInfo, srcOpts, dstOpts)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

}

func (web *webAPIHandlers) Fetch(w http.ResponseWriter, r *http.Request) {
	Authorization := r.Header.Get("Authorization")
	baseUrl := r.RequestURI
	credential := globalServerConfig.GetCredential()
	signature := hmacSha1(baseUrl, credential.SecretKey)
	token := credential.AccessKey + ":" + signature
	if Authorization == "QBox "+token {
		//some check

	} else {
		//http.Error(w,"authorization error",401)
		//return
	}
	vars := mux.Vars(r)
	fetchURL, _ := base64.URLEncoding.DecodeString(vars["EncodedURL"])
	EntryURI, _ := base64.URLEncoding.DecodeString(vars["EncodedEntryURI"])
	objectList := strings.Split(string(EntryURI), ":")

	bucket := objectList[0]
	key := objectList[1]
	objectInfo, err := fetchFile(string(fetchURL), bucket, key)

	if err != nil {
		http.Error(w, string(err.Error()), 400)
		return
	}

	fetchResponse := FetchResponse{Fsize: objectInfo.Size, Hash: objectInfo.ETag, MimeType: objectInfo.ContentType, Key: key}
	jsonResponse, err := json.Marshal(fetchResponse)
	if err != nil {
		errMsg := ErrorMessage{Error: "metadata parse error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 599)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonResponse)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (web *webAPIHandlers) GetMetadata(w http.ResponseWriter, r *http.Request) {
	Authorization := r.Header.Get("Authorization")
	baseUrl := r.RequestURI
	credential := globalServerConfig.GetCredential()
	signature := hmacSha1(baseUrl, credential.SecretKey)
	token := credential.AccessKey + ":" + signature
	if Authorization == "QBox "+token {
		//some check

	} else {
		//http.Error(w,"authorization error",401)
		//return
	}
	vars := mux.Vars(r)
	object, _ := base64.URLEncoding.DecodeString(vars["object"])
	objectList := strings.Split(string(object), ":")
	if len(objectList) != 2 {
		errMsg := ErrorMessage{Error: "entry parse error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 400)
		return
	}
	bucket := objectList[0]
	key := objectList[1]
	fmt.Println("get Media",bucket, key)

	ctx := MingdaoNewContext(r, w, "GetObject")
	//var rs *HTTPRangeSpec
	var opts ObjectOptions
	var err error
	var objectInfo ObjectInfo

	if globalMingdaoFs.Mode == 3 {
		objectInfo, err = globalMingdaoFs.S3Api.GetObjectInfo(ctx, bucket, key, opts)
	} else {
		objectInfo, err = globalMingdaoFs.GetObjectInfo(ctx, bucket, key, opts)
	}

	if err != nil {
		errMsg := ErrorMessage{Error: "file not found error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 612)
		return
	}
	//defer gr.Close()
	//fmt.Print(objectInfo)

	statsResponse := StatsResponse{Fsize: objectInfo.Size, Hash: objectInfo.ETag, MimeType: objectInfo.ContentType, Type: 0, PutTime: objectInfo.ModTime.Unix() * 1000 * 10000}

	jsonResponse, err := json.Marshal(statsResponse)
	if err != nil {
		errMsg := ErrorMessage{Error: "metadata parse error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 599)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(jsonResponse)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)

}

func (web *webAPIHandlers) MindaoOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("access-control-allow-headers", "X-File-Name, X-File-Type, X-File-Size")
	w.Header().Set("access-control-allow-methods", "OPTIONS, HEAD, POST")
	w.Header().Set("access-control-allow-origin", "*")
	w.Header().Set("access-control-expose-headers", "X-Log, X-Reqid")
	w.Header().Set("access-control-max-age", "2592000")
	w.Header().Set("cache-control", "no-store, no-cache, must-revalidate")
	w.Header().Set("content-length", "")
	w.Header().Set("pragma", "no-cache")
	w.WriteHeader(http.StatusOK)
}

func (web *webAPIHandlers) MingdaoMultipartUpload(w http.ResponseWriter, r *http.Request) {
	response := MutilPartResponse{}
	uuid := MustGetUUID()
	file, err := os.Create(globalTmpPrefix + "multitmp/" + uuid)
	defer file.Close()
	if err != nil {
		http.Error(w, "create file error"+err.Error(), 400)
		return
	}
	_, err = io.Copy(file, r.Body)
	ExpireMutilPartFile(file.Name())

	base64UUID := base64.URLEncoding.EncodeToString([]byte(uuid))
	response.Offset = r.ContentLength

	scheme := "http://"
	if r.TLS != nil || globalIsTls == true {
		scheme = "https://"
	}

	if globalDomainNames != nil {
		response.Host = strings.Join([]string{scheme, globalDomainNames[0], "/mingdao/upload"}, "")
	} else {
		response.Host = strings.Join([]string{scheme, r.Host, "/mingdao/upload"}, "")
	}
	response.Ctx = base64UUID

	var buf bytes.Buffer
	crcfile, _ := os.Open(file.Name())
	defer crcfile.Close()
	buf.ReadFrom(crcfile)
	response.Crc32 = getcrc32(buf.String())

	h := sha1.New()
	h.Write(buf.Bytes())
	checksum := base64.URLEncoding.EncodeToString(h.Sum(nil))
	response.Checksum = checksum
	response.Expire = int(time.Now().Unix()) + 3600*3

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		errMsg := ErrorMessage{Error: "metadata parse error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 599)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonResponse)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (web *webAPIHandlers) MingdaoBputUpload(w http.ResponseWriter, r *http.Request) {
	response := MutilPartResponse{}

	vars := mux.Vars(r)
	ctx := vars["ctx"]
	h := crc32.NewIEEE()
	sha := sha1.New()

	response.Ctx = vars["ctx"]
	uuid, _ := base64.URLEncoding.DecodeString(ctx)
	nextOffset, _ := strconv.Atoi(vars["nextChunkOffset"])

	file, err := os.OpenFile(globalTmpPrefix+"multitmp/"+string(uuid), os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		http.Error(w, "bput open file error"+err.Error(), 400)
		return
	}

	defer file.Close()
	body := io.TeeReader(r.Body, h)

	body = io.TeeReader(body, sha)

	_, err = io.Copy(file, body)

	if err != nil {
		http.Error(w, "bput  copy  "+err.Error(), 400)
	}
	ExpireMutilPartFile(file.Name())

	response.Offset = r.ContentLength + int64(nextOffset)
	scheme := "http://"
	if r.TLS != nil || globalIsTls == true {
		scheme = "https://"
	}

	if globalDomainNames != nil {
		response.Host = strings.Join([]string{scheme, globalDomainNames[0], "/mingdao/upload"}, "")
	} else {
		response.Host = strings.Join([]string{scheme, r.Host, "/mingdao/upload"}, "")
	}

	response.Crc32 = int(h.Sum32())

	checksum := base64.URLEncoding.EncodeToString(sha.Sum(nil))
	response.Checksum = checksum

	response.Expire = int(time.Now().Unix()) + 3600*3

	jsonResponse, err := json.Marshal(response)

	if err != nil {
		errMsg := ErrorMessage{Error: "metadata parse error"}
		err, _ := json.Marshal(errMsg)
		http.Error(w, string(err), 599)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonResponse)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func (web *webAPIHandlers) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	returnBody, err := getMagicargs(token)
	if err != nil {
		http.Error(w, "token parse error", 400)
		return
	}

	fileArg := qiniuRequestArg{fileName: "null", fileExt: "null", filePath: "null", originalFileName: "null", serverName: "null"}

	//response:=MutilPartResponse{}
	bucket := returnBody.Scope

	vars := mux.Vars(r)
	filesize, _ := strconv.Atoi(vars["filesize"])
	fileArg.fsize = vars["filesize"]
	params := vars["param"]
	list := strings.Split(params, "/")
	key := ""
	for i, v := range list {
		if strings.Contains(v, "key") {
			bytekey, _ := base64.URLEncoding.DecodeString(list[i+1])
			key = string(bytekey)
		}
	}

	fileArg.key = key
	fileArg.bucket = returnBody.Scope
	filePathInfo := formatkey(fileArg.key)
	fileArg.fileName = filePathInfo[1]

	//io.LimitReader(r.Body, int64(tenKB))
	//var ctxData []byte
	ctx, err := ioutil.ReadAll(r.Body)
	//n,err:=io.ReadFull(r.Body,ctxData)
	ctxList := strings.Split(string(ctx), ",")
	tmpPath := globalTmpPrefix + "tmp/" + mustGetUUID()
	file, err := os.Create(tmpPath)
	for _, v := range ctxList {
		filename, _ := base64.URLEncoding.DecodeString(v)
		crcfile, _ := os.Open(globalTmpPrefix + "multitmp/" + string(filename))
		io.Copy(file, crcfile)
		crcfile.Close()
		defer os.Remove(globalTmpPrefix + "multitmp/" + string(filename))
		//fmt.Println(err)
	}
	file.Close()
	//文件hash计算
	var pReader *PutObjReader
	file, _ = os.OpenFile(tmpPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 066)

	defer file.Close()
	defer fsRemoveFile(context.Background(), tmpPath)

	var reader io.Reader = file

	hashReader, err := hash.NewReader(reader, int64(filesize), "", "", int64(filesize), globalCLIContext.StrictS3Compat)
	if err != nil {
		http.Error(w, "hash error", 400)
		return
	}

	var opts ObjectOptions
	opts, err = putOpts(context.Background(), r, bucket, key, nil)

	pReader = NewPutObjReader(hashReader, nil, nil)

	var objInfo ObjectInfo
	objInfo, err = globalMingdaoFs.PutObject(context.Background(), bucket, key, pReader, opts)
	fileArg.etag = objInfo.ETag
	if err != nil {
		fmt.Println(err)
		fmt.Println(err.Error())
		http.Error(w, err.Error(), 400)
		return
	}

	for k, v := range returnBody.ReturnBody {
		_ = returnBody.ReturnBody[k]
		//fmt.Print(m)
		returnBody.ReturnBody[k] = formatResponseArg(&fileArg, r.Form, v)
	}
	result, _ := json.MarshalIndent(returnBody.ReturnBody, "", "")

	//response := CompleteMutilResponse{Bucket: bucket, Etag: objInfo.ETag, FileName: key, Fsize: filesize, Key: key}

	//jsonResponse, err := json.Marshal(response)
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	//w.WriteHeader(200)

}

func (web *webAPIHandlers) MindaoUpload(w http.ResponseWriter, r *http.Request) {
	req, err := CheckparseMultiPart(r)
	ctx := MingdaoNewContext(r, w, "PutObject")

	if err != nil {
		http.Error(w, "check error", 400)
		return
	}
	fileArg := qiniuRequestArg{}
	//key := req.Form.Get("key")
	token := req.Form.Get("token")
	returnBody, err := getMagicargs(token)
	if err != nil {
		http.Error(w, "token parse error", 400)
		return
	}
	fileArg.bucket = returnBody.Scope
	if isReservedOrInvalidBucket(fileArg.bucket, false) {
		writeWebErrorResponse(w, errInvalidBucketName)
		return
	}

	//读取数据拿大小
	//文件hash计算
	var pReader *PutObjReader
	reader := getFormData(r.MultipartForm, &fileArg)
	//fileArg.key=strings.ReplaceAll(r.Form.Get("key"),":","/")
	fileArg.key = r.Form.Get("key")
	if fileArg.fileSize < 0 {
		writeWebErrorResponse(w, errSizeUnspecified)
		return
	}
	filePathInfo := formatkey(fileArg.key)
	fileArg.filePath = filePathInfo[0]
	fileArg.fileName = filePathInfo[1]
	fmt.Printf("打印fileArg：%v \n", fileArg)

	fileArg.fsize = strconv.Itoa(int(fileArg.fileSize))
	fileArg.ext = path.Ext(fileArg.fileName)


	for _,v := range blockExt {
		if strings.ToLower(fileArg.ext)==v{
			http.Error(w, "invalid fileExt ", 400)
			return
		}
	}


	metadata, err := extractMetadata(context.Background(), r)
	if err != nil {
		http.Error(w, "metadata error", 400)
		return
	}

	var opts ObjectOptions
	opts, err = putOpts(ctx, r, fileArg.bucket, fileArg.key, metadata)
	if err != nil {
		writeErrorResponseHeadersOnly(w, toAPIError(ctx, err))
		return
	}

	//fmt.Print(returnBody,bucket,metadata)
	//defer fsRemoveFile(context.Background(), fileArg.tmpPath)

	//var reader io.Reader = file

	hashReader, err := hash.NewReader(reader, int64(fileArg.fileSize), "", "", int64(fileArg.fileSize), globalCLIContext.StrictS3Compat)
	if err != nil {
		http.Error(w, "hash error", 400)
		return
	}
	pReader = NewPutObjReader(hashReader, nil, nil)
	var objInfo ObjectInfo

	objInfo, err = globalMingdaoFs.PutObject(context.Background(), fileArg.bucket, fileArg.key, pReader, opts)
	fileArg.etag = objInfo.ETag

	if err != nil {
		fmt.Println(err)
		http.Error(w, "put error", 400)
		return
	}

	for k, v := range returnBody.ReturnBody {
		_ = returnBody.ReturnBody[k]
		//fmt.Print(m)
		returnBody.ReturnBody[k] = formatResponseArg(&fileArg, r.Form, v)
	}
	result, _ := json.MarshalIndent(returnBody.ReturnBody, "", "")

	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	w.Header().Set("Content-Type", "application/json")
	w.Write(result)
	//w.WriteHeader(200)

	//w.WriteHeader(200)
}

func formatResponseArg(filearg *qiniuRequestArg, form url.Values, value interface{}) interface{} {

	switch value {

	case "$(name)":
		return filearg.fileName
	case "$(key)":
		return filearg.key
	case "$(etag)", "$(hash)":
		return filearg.etag
	case "$(fsize)":
		return filearg.fsize
	case "$(bucket)":
		return filearg.bucket
	default:
		valueType := reflect.TypeOf(value).String()
		if valueType == "string" {
			var valid = regexp.MustCompile("\\$\\((.*?)\\)")
			//b:=[]byte("(x:file)")
			v := valid.FindStringSubmatch((value).(string))
			if v != nil {
				return form.Get(v[1])
			} else {
				return value
			}
		} else {
			return value
		}
	}
}

func getMagicargs(token string) (qiniuBody, error) {
	s := strings.Split(token, ":")
	decodedFlags, _ := base64.URLEncoding.DecodeString(s[2])
	var mapResult qiniuRequest
	err := json.Unmarshal(decodedFlags, &mapResult)
	if err != nil {
		error := ErrorCode{1, "token check error"}
		return qiniuBody{}, &error
	}
	var qiniuData qiniuBody
	if mapResult.ReturnBody != "" {
		m, ok := gjson.Parse(mapResult.ReturnBody).Value().(map[string]interface{})
		if !ok {
			error := ErrorCode{1, "token parse error"}
			return qiniuBody{}, &error
			// not a map
		}
		qiniuData = qiniuBody{m, mapResult.Scope, mapResult.Deadline}
	} else {
		qiniuData = qiniuBody{nil, mapResult.Scope, mapResult.Deadline}
	}
	return qiniuData, nil
}

func CheckparseMultiPart(r *http.Request) (*http.Request, error) {
	/**
	  底层通过调用multipartReader.ReadForm来解析
	  如果文件大小超过maxMemory,则使用临时文件来存储multipart/form中文件数据
	*/
	r.ParseMultipartForm(32 << 20)
	//key := r.Form.Get("key")
	token := r.Form.Get("token")
	s := strings.Split(token, ":")
	credential := globalServerConfig.GetCredential()
	if len(s) == 3 {
		accessKey := s[0]
		encodedFlags := s[2]
		//decodedFlags, _ := base64.StdEncoding.DecodeString(s[2])
		encodedSign := s[1]
		if accessKey == credential.AccessKey && hmacSha1(encodedFlags, credential.SecretKey) == encodedSign {
			//fmt.Printf("%s           %s,", encodedSign, encodedFlags)
			//getFormData(r.MultipartForm, key)
		} else {
			error := ErrorCode{1, "token check error"}
			return nil, &error
		}
	} else {
		error := ErrorCode{1, "token check error"}
		return nil, &error
	}
	return r, nil
}

func TokenCheck(baseUrl string, token string) bool {

	credential := globalServerConfig.GetCredential()

	signature := hmacSha1(baseUrl, credential.SecretKey)

	downloadToken := credential.AccessKey + ":" + signature

	if downloadToken == token {
		return true
	} else {
		return false
	}
}

func getFormData(form *multipart.Form, qiniu *qiniuRequestArg) io.Reader {
	//获取 multi-part/form中的文件数据
	//var fsTmpObjPath string
	for _, v := range form.File {
		for i := 0; i < len(v); i++ {
			fmt.Println("getdata ",v[i].Filename)
			qiniu.fileSize += int(v[i].Size)
			qiniu.originalFileName = v[i].Filename
			file, _ := v[i].Open()
			//defer file.Close()
			//tempObj := mustGetUUID()
			return file
			//fsTmpObjPath = pathJoin(globalMingdaoFs.fsPath, minioMetaTmpBucket, globalMingdaoFs.fsUUID, tempObj)
			//
			//cur, err := os.Create(fsTmpObjPath)
			//defer cur.Close()
			//if err == nil {
			//	io.Copy(cur, file)
			//	fmt.Print(t1, t2)
			//}
			//fmt.Println("file-content",string(buf))
			//fmt.Println()
		}
	}
	return nil
}

func hmacSha1(encodedFlags string, secretKey string) string {
	key := []byte(secretKey)
	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(encodedFlags))
	return base64.URLEncoding.EncodeToString(mac.Sum(nil))
}

func formatkey(key string) []string {

	index := strings.LastIndex(key, "/")

	file := []string{key[:index+1], key[index+1 : len(key)-1]}

	return file
}

func fetchFile(url string, bucket string, key string) (*ObjectInfo, error) {
	mutex.Lock()
	resp, err := grab.Get(globalTmpPrefix+"/fetchtmp", url)
	mutex.Unlock()

	if err != nil {
		error := ErrorCode{1, "download file error"}
		fmt.Println("download file error ", err, url)
		return nil, &error
	}

	var pReader *PutObjReader
	file, _ := os.OpenFile(resp.Filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 066)

	defer file.Close()
	defer fsRemoveFile(context.Background(), resp.Filename)

	var reader io.Reader = file

	var opts ObjectOptions
	//opts, err = putOpts(context.Background(), nil, bucket, key, nil)

	hashReader, err := hash.NewReader(reader, int64(resp.Size), "", "", int64(resp.Size), globalCLIContext.StrictS3Compat)
	if err != nil {
		error := ErrorCode{1, "hash error"}
		return nil, &error
	}
	fileArg := qiniuRequestArg{}
	fileArg.bucket = bucket
	if isReservedOrInvalidBucket(fileArg.bucket, false) {
		error := ErrorCode{1, "bucket check error"}
		return nil, &error
	}

	fileArg.tmpPath = resp.Filename

	fileArg.ext = path.Ext(fileArg.tmpPath)
	fileArg.key = key

	fileArg.fsize = strconv.Itoa(int(fileArg.fileSize))
	filePathInfo := formatkey(fileArg.key)
	fileArg.filePath = filePathInfo[0]
	fileArg.fileName = filePathInfo[1]

	pReader = NewPutObjReader(hashReader, nil, nil)
	fileArg.etag = pReader.MD5CurrentHexString()
	objInfo, err := globalMingdaoFs.PutObject(context.Background(), bucket, key, pReader, opts)
	if err != nil {
		error := ErrorCode{1, "put error"}
		return nil, &error
	}

	return &objInfo, nil

}
