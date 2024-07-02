package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var Log *log.Logger

const stdout = false

func Assert(cond bool, reason string) {
    if (!cond) {
        log.Panicf("Assert fail: %v\n", reason)
    }
}

func init() {
	flags := log.Ltime | log.Lmicroseconds | log.Lshortfile
	// 初始化 Logger
	if stdout {
		Log = log.New(os.Stdout, "", flags)
	} else {
		files, err := os.ReadDir(".")
		if err != nil {
			fmt.Println("Error reading directory:", err)
			return
		}
		num := 0
		for _, file := range files {
			if !file.IsDir() {
				name := file.Name()
				if !strings.Contains(name, "log_") {
					continue
				}
				start := strings.LastIndex(name, "_")
				end := strings.Index(name, ".")
				thenum, err := strconv.Atoi(name[start+1 : end])
				if err != nil {
					log.Fatalln(err)
				}
				if thenum > num {
					num = thenum
				}
			}
		}

		filename := fmt.Sprintf("./log_%v_%v.txt", time.Now().Format("2006_01_02"), num+1)
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


