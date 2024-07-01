package raft

import (
	"log"
    "os"
    "time"
    "fmt"
)

// Debugging
const Debug = true
var Log *log.Logger

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func init() {
    filename := fmt.Sprintf("./log_%v.txt", time.Now().Format("2006_01_02"))
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
    flags := log.Ltime | log.Lmicroseconds | log.Lshortfile

    // 初始化 Logger
    Log = log.New(file, "", flags)
}
