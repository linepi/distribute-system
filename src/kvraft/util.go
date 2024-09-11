package kvraft

import (
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"
)

var Log *log.Logger
var stdout = true

func Assert(cond bool, reason string) {
	if !cond {
		if len(reason) > 0 {
			Log.Panicf("Assert Failed: %s, Reason: %s\n", debug.Stack(), reason)
		} else {
			Log.Panicf("Assert Failed: %s\n", debug.Stack())
		}
	}
}

func Panic() {
	Assert(false, "Panic")
}

func init() {
	if os.Getenv("RAFT_STDOUT") != "" {
		stdout = true
	}

	flags := log.Ltime | log.Lmicroseconds | log.Lshortfile
	// 初始化 Logger
	if stdout {
		Log = log.New(os.Stdout, "", flags)
	} else {
		now := time.Now()
		// 标准的时间格式化，到秒
		baseFormat := now.Format("2006-01-02-15-04-05")
		// 获取纳秒部分并转换为微秒
		microseconds := now.Nanosecond() / 1000
		// 组合成最终的时间戳字符串
		timestamp := fmt.Sprintf("%s_%06d", baseFormat, microseconds)

		logDir := "./kvlogs"
		value, exists := os.LookupEnv("RAFT_LOG_DIR") // 注意：从 Go 1.17 开始，推荐使用 LookupEnv
		if exists {
			logDir = value
			if err := os.MkdirAll(logDir, 0755); err != nil {
				fmt.Println("Error creating Log directory:", err)
				return
			}
		}
		filename := fmt.Sprintf("%s/log_%s.txt", logDir, timestamp)
		fmt.Printf("Log path: %s\n", filename)

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
