package cmd

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"github.com/robfig/cron"
)

func ExpireMutilPartFile(filepath string)  {
	c := cron.New()
	c.AddFunc("0 0 3/1 * * ? ", func() {

		err := os.Remove(filepath)
		if err!=nil{
			fmt.Println("remove file err  ",filepath,err)
		}
		c.Stop()

	})
}

func ExpireCacheFile(redisKey string,redisField string,filepath string)  {
	c := cron.New()
	c.AddFunc("0 0 8/1 * * ? ", func() {
		Redisclient.HDel(redisKey,redisField).Result()

		err := os.Remove(filepath)
		if err!=nil{
			fmt.Println("remove file err  ",filepath,err)
		}
		c.Stop()

	})
}

func getcrc32(check_str string)int  {
	ieee := crc32.NewIEEE()
	io.WriteString(ieee, check_str)
	return int(ieee.Sum32())
}