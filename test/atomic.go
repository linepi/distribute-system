package main

import (
	// "os"
	// "log"
	// "time"
	// "strconv"
	// "encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

func main() {
	var val int32
	val = 0
	var wg sync.WaitGroup
	var arr []int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				arr = append(arr, atomic.LoadInt32(&val))	
				atomic.AddInt32(&val, 1)
			}
		}()
	}
	wg.Wait()
	fmt.Printf("%v\n", val)
	fmt.Printf("arr len %v\n", len(arr))
	// for i := 0; i < 100000; i++ {
	// 	if i != int(arr[i]) {
	// 		fmt.Printf("error %v != %v\n", i, arr[i])
	// 	}
	// }
}