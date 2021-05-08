package cmd

import (
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/go-redis/redis"
)

const dirpath = globalTmpPrefix + "cache/"
const prefix = "st_file_"

var Redisclient *redis.Client

func initRedisCache(ip string) {
	opt, err := redis.ParseURL(ip)
	if err != nil {
		panic(err)
	}
	Redisclient = redis.NewClient(opt)
	deleteAllCache()
}

func getImageCachePath(bucket string, key string, urlreq string) (string, error) {
	if Redisclient != nil {
		val, err := Redisclient.HGetAll(prefix + bucket + key).Result()
		if len(val) > 0 && err == nil {
			result, _ := val[urlreq]
			if result != "" {
				return result, nil
			} else {
				error := ErrorCode{1, "no cache"}
				return "", &error
			}
		}
		error := ErrorCode{1, "no cache"}
		return "", &error
	} else {
		error := ErrorCode{1, "no cache"}
		return "", &error
	}
}

func saveImageCache(tmppath string, bucket string, key string, ext string, urlreq string) error {
	if Redisclient != nil {
		//base64Key := base64.URLEncoding.EncodeToString([]byte(key))
		path := dirpath + MustGetUUID() + "." + ext
		file, err := os.Create(path)
		defer file.Close()
		if err != nil {
			return err
		}
		reader, _ := os.Open(tmppath)
		defer reader.Close()
		_, err = io.Copy(file, reader)
		Redisclient.HSet(prefix+bucket+key, urlreq, path).Result()
		//Redisclient.Expire(prefix + bucket + key,30000)
		ExpireCacheFile(prefix+bucket+key, urlreq, path)
		return nil
	} else {
		return nil
	}
}

func deleteCache(bucket string, key string) {
	if Redisclient != nil {
		pathList, err := Redisclient.HGetAll(prefix + bucket + key).Result()
		if err != nil {
			return
		}
		for val, key := range pathList {
			go func() {
				os.Remove(val)
				Redisclient.HDel(prefix + bucket + key)
			}()
		}
	} else {
		return
	}
}

func deleteAllCache() {
	dir, _ := ioutil.ReadDir(dirpath)
	for _, d := range dir {
		os.RemoveAll(path.Join([]string{dirpath, d.Name()}...))
	}
}
