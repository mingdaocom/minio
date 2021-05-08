package cmd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

type Configs map[string]json.RawMessage

var configPath = getConfigPath()

type S3Config struct {
	Mode            int                    `json:"mode"` //1为s3 模式
	AccessKeyID     string                 `json:"accessKeyID"`
	SecretAccessKey string                 `json:"secretAccessKey"`
	BucketEndPoint  string                 `json:"bucketEndPoint"`
	BucketName      map[string]interface{} `json:"bucketName"`
	Region          string                 `json:"region"`
}

type BucketName struct {
	Mdmedia string `json:"mdmedia"`
	mdpic   string `json:"mdpic"`
	mdpub   string `json:"mdpub"`
	mdoc    string `json:"mdoc"`
}

var confs Configs

var instanceOnce sync.Once

//从配置文件中载入json字符串
func LoadConfig(path string) (Configs, *S3Config) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		log.Print("load config conf failed: ", err)
	}
	mainConfig := S3Config{}
	err = json.Unmarshal(buf, &mainConfig)
	if err != nil {
		log.Print("decode config file failed:", string(buf), err)
	}
	allConfigs := make(Configs, 0)
	err = json.Unmarshal(buf, &allConfigs)
	if err != nil {
		log.Print("decode config file failed:", string(buf), err)
	}

	return allConfigs, &mainConfig
}

//初始化 可以运行多次
func SetConfig(path string) {
	allConfigs, mainConfig := LoadConfig(path)
	configPath = path
	GlobalS3Conf = mainConfig
	confs = allConfigs
}

// 初始化，只能运行一次
func S3ConfigInit(path string) *S3Config {
	if GlobalS3Conf != nil && path != configPath {
		log.Printf("the config is already initialized, oldPath=%s, path=%s", configPath, path)
	}
	instanceOnce.Do(func() {
		allConfigs, mainConfig := LoadConfig(path)
		configPath = path
		GlobalS3Conf = mainConfig
		confs = allConfigs
	})

	return GlobalS3Conf
}

//初始化配置文件 为 struct 格式
func Instance() *S3Config {
	if GlobalS3Conf == nil {
		S3ConfigInit(configPath)
	}
	return GlobalS3Conf
}

//初始化配置文件 为 map格式
func AllConfig() Configs {
	if GlobalS3Conf == nil {
		S3ConfigInit(configPath)
	}
	return confs
}

//获取配置文件路径
func S3ConfigPath() string {
	return configPath
}

//根据key获取对应的值，如果值为struct，则继续反序列化
func (cfg Configs) GetConfig(key string, config interface{}) error {
	c, ok := cfg[key]
	if ok {
		return json.Unmarshal(c, config)
	} else {
		return fmt.Errorf("fail to get cfg with key: %s", key)
	}
}

func getConfigPath() string {
	dir, _ := os.Getwd()
	var cwdPath string
	cwdPath = path.Join(dir, "s3-config.json") // the the main function file directory
	return cwdPath
}

//func main() {
//	path := S3ConfigPath()
//	fmt.Println("path: ", path)
//	S3ConfigInit(path)
//	value := confs["port"]
//	fmt.Println(string(value))
//}
