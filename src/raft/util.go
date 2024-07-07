package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

var Log *log.Logger

const stdout = false

func Assert(cond bool, reason string) {
	if !cond {
		log.Panicf("Assert fail: %v\n", reason)
	}
}

func init() {
	flags := log.Ltime | log.Lmicroseconds | log.Lshortfile
	// 初始化 Logger
	if stdout {
		Log = log.New(os.Stdout, "", flags)
	} else {
    now := time.Now()
    // 标准的时间格式化，到秒
    baseFormat := now.Format("2006_01_02_15_04_05")
    // 获取纳秒部分并转换为微秒
    microseconds := now.Nanosecond() / 1000
    // 组合成最终的时间戳字符串
    timestamp := fmt.Sprintf("%s_%06d", baseFormat, microseconds)

		filename := fmt.Sprintf("./logs/log_%s.txt", timestamp)
		file, err := os.OpenFile(
			filename,
			os.O_APPEND|os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			0644)

		if err != nil {
			if os.IsExist(err) {
				file, err = os.OpenFile(filename, os.O_WRONLY, 0644)
				if err != nil {
					fmt.Println("Error opening existing file:", err)
					return
				}
			} else {
				fmt.Println("Error creating file:", err)
				return
			}
		}
		Log = log.New(file, "", flags)
	}
}
