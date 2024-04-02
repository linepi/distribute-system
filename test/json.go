package main

import "encoding/json"
import "os"
import "log"
import "time"
import "fmt"
import "strconv"

type KeyValue struct {
	Key   string
	Value string
}

type InterFiles struct {
	splitsize int // max kv store in a internal file
}

var interFiles InterFiles

func NewInterFiles() InterFiles {
	fmt.Println("NewInterFiles...")
	interFiles.splitsize = 1000
	return interFiles
}


func main() {
	inter_file := "interfile-"
	var mappedkv []KeyValue	

	for i := 0; i < 10000; i++ {
		mappedkv = append(mappedkv, KeyValue{"123", "456"})
	}

	file, err := os.Create(inter_file)
	if err != nil {
		log.Fatalf("cannot open %v: %s", inter_file, err)
	}
	enc := json.NewEncoder(file)
	fmt.Println("Start")
  for i := 1; i <= len(mappedkv); i++ {
		time.Sleep(time.Millisecond)
		kv := mappedkv[i-1]
    err := enc.Encode(&kv)
		if err != nil {
			panic("json encode error")
		}
		if i % 1000 == 0 {
			file, err = os.Create(inter_file + strconv.Itoa(i))
			if err != nil {
				log.Fatalf("cannot open %v: %s", inter_file, err)
			}
			enc = json.NewEncoder(file)
		}
	}
}