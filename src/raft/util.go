package raft

import (
	"log"
    "os"
    "time"
    "fmt"
)

var Log *log.Logger
const stdout = false

func init() {
    flags := log.Ltime | log.Lmicroseconds | log.Lshortfile
    // 初始化 Logger
    if stdout {
        Log = log.New(os.Stdout, "", flags)
    } else {
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
        Log = log.New(file, "", flags)
    }
}
