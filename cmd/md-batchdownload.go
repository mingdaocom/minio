package cmd

import (
	"archive/zip"
	"context"
	"crypto/md5"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/tidwall/gjson"
	"io"
	"os"
	"strings"
	"time"
)

const Downloadprefix = "H:BatchDownload"

var BatchDownloadRedis *redis.Client

type downloadInfo struct {
	zipfilename    string
	downloadDetail []downloadDetail
}

type downloadDetail struct {
	bucket string
	key    string
	name   string
	url    string
}

func main() {
	initDownloadredis("132.232.27.116:6379")
}

func initDownloadredis(ip string) {
	opt, err := redis.ParseURL(ip)
	if err != nil {
		panic(err)
	}
	BatchDownloadRedis = redis.NewClient(opt)
}

func getDownloadFilesByCacheId(CacheId string) (ObjectInfo, error) {
	var key = strings.ToLower(Downloadprefix) + strings.ToLower(CacheId)
	var DownloadInfo downloadInfo
	if BatchDownloadRedis != nil {
		result, err := BatchDownloadRedis.Get(key).Result()
		if err != nil {
			fmt.Println("downloadInfo error")
			error := ErrorCode{1, "get downloadInfo error"}
			return ObjectInfo{}, &error
		} else {
			filename := gjson.Get(result, "filename")
			DownloadInfo.zipfilename = filename.String()
			DownloadInfo.downloadDetail = []downloadDetail{}
			//bucket := gjson.Get(result, "domains")
			domain := gjson.Get(result, "domains")
			domain.ForEach(func(key, value gjson.Result) bool {
				//println(key.String())
				path := value.Get("files")
				needdown := value.Get("private")
				for _, v := range path.Array() {
					var DownloadDetail downloadDetail
					DownloadDetail.bucket = key.String()
					filekey := v.Array()[0]
					DownloadDetail.key = filekey.String()
					filename := v.Array()[1]
					DownloadDetail.name = strings.TrimLeft(filename.String(), "/")
					if needdown.Bool() == false {
						DownloadDetail.url = value.Get("linkUrl").Str
					}
					DownloadInfo.downloadDetail = append(DownloadInfo.downloadDetail, DownloadDetail)

				}
				//	println(path.String())
				return true // keep iterating
			})
			zipInfo, _ := downloadFilesAndZip(DownloadInfo)

			return zipInfo, nil
		}
	}
	error := ErrorCode{1, "no downloadredis found"}
	return ObjectInfo{}, &error
}

func downloadFilesAndZip(DownloadInfo downloadInfo) (ObjectInfo, error) {
	tempObj := mustGetUUID()
	tmppath := globalTmpPrefix + tempObj + ".zip"
	zipfile, err := os.Create(tmppath)
	defer zipfile.Close()
	zipwriter := zip.NewWriter(zipfile)

	if err != nil {
		fmt.Println("Create error: ", err)
	}

	for _, v := range DownloadInfo.downloadDetail {
		var opts ObjectOptions
		var err error
		var gr *GetObjectReader
		var rs *HTTPRangeSpec

		fh := &zip.FileHeader{
			Name:     v.name,
			Modified: time.Now(),
		}
		if v.url == "" {
			if globalMingdaoFs.Mode == 3 {
				gr, err = globalMingdaoFs.S3GetObjectNInfo(context.Background(), v.bucket, v.key, rs, nil, readLock, opts, 0)
			} else {
				gr, err = globalMingdaoFs.GetObjectNInfo(context.Background(), v.bucket, v.key, rs, nil, readLock, opts)
			}
			if err != nil {
				error := ErrorCode{1, "download file error"}
				return ObjectInfo{}, &error
			}
			fileWriter, err := zipwriter.CreateHeader(fh)
			if err != nil {
				error := ErrorCode{1, "create zipfile error"}
				return ObjectInfo{}, &error
			}
			fmt.Println("创建 " + gr.ObjInfo.Name)

			_, err = io.Copy(fileWriter, gr.pReader)
			if err != nil {
				error := ErrorCode{1, "zip  file error"}
				return ObjectInfo{}, &error
			}
		} else {
			fileWriter, err := zipwriter.CreateHeader(fh)
			if err != nil {
				error := ErrorCode{1, "create zipfile error"}
				return ObjectInfo{}, &error
			}
			_, err = fileWriter.Write([]byte("[InternetShortcut]\nURL=" + v.url))
		}
	}
	err = zipwriter.Close()
	fmt.Println("Close error: ", err)
	var objectInfo ObjectInfo
	objectInfo.Name = "批量下载.zip"
	objectInfo.Size = GetFileSize(tmppath)
	objectInfo.ContentType = ""
	objectInfo.path = tmppath
	file, inerr := os.Open(tmppath)
	defer file.Close()
	if inerr == nil {
		md5h := md5.New()
		io.Copy(md5h, file)
		objectInfo.ETag = string(md5h.Sum([]byte("")))
	}
	objectInfo.ModTime = time.Now()
	objectInfo.ContentType = "application/octet-stream"
	objectInfo.ContentEncoding = "binary"
	return objectInfo, nil
}
