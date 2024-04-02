package mr

import (
	"log"
	"os"
	"io"
	"strconv"
	"net/rpc"
	"hash/fnv"
	"encoding/json"
	"time"
	"sync/atomic"
	"fmt"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const (
	WORKER_TYPE_MAP = 0
	WORKER_TYPE_REDUCE = 1
	INTER_FILE_SPLIT_SIZE = 5000
	inter_file_prefix = "824-inter-"
	reduceNum = 10
	maxMapThreadNum = 1
	maxReduceThreadNum = 1
)

func get_interfile_name(mid int, rid int) string {
	return inter_file_prefix + strconv.Itoa(mid) + "-" + strconv.Itoa(rid)
}

type WorkerStruct struct {
	Type int
	ReduceObj Reduce
	MapObj Map
}

type Reduce struct {
	Id int
	Done bool // Done or not from master
	RequestFile []int // map file Id that is requested this time
	FinishedMapFile []int // a bucket, [i] == 1 means has dealt with the map i's file
}

type Map struct {
	Id int
	File string 
}

var curMapThreadNum int32
var curReduceThreadNum int32

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func MapFunc(arg *Map, mapf func(string, string) []KeyValue) {
	filename := arg.File
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	mappedkv := mapf(filename, string(content))

	encoders := []*json.Encoder{}
	for i := 0; i < reduceNum; i++ {
		// prefix + mapid + reduceid
		file, err = os.Create(get_interfile_name(arg.Id, i))
		if err != nil {
			log.Fatalf("cannot open %v: %s", get_interfile_name(arg.Id, i), err)
		}
		enc := json.NewEncoder(file)
		encoders = append(encoders, enc)
	}

  for i := 1; i <= len(mappedkv); i++ {
		kv := mappedkv[i-1]
    err := encoders[ihash(kv.Key) % reduceNum].Encode(&kv)
		if err != nil {
			panic("json encode error")
		}
	}
	call("Master.MapFinish", arg, arg)
	atomic.AddInt32(&curMapThreadNum, -1)
}

func ReduceFunc(arg *Reduce, reducef func(string, []string) string) {
	// printf("reduce launch: %v\n", arg)
	m := make(map[string][]string)

	for {
		call("Master.ReduceRequest", arg, arg)
		if arg.Done {
			break
		} 
		if len(arg.RequestFile) == 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			printf("RequestFile %v\n", arg.RequestFile)
		}
		for _, mid := range arg.RequestFile {
			if mid >= 0 && mid < len(arg.FinishedMapFile) && arg.FinishedMapFile[mid] == 0 {
				file, err := os.Open(get_interfile_name(mid, arg.Id))
				if err != nil {
					log.Fatalf("cannot open %v: %s", get_interfile_name(mid, arg.Id), err)
				}
				dec := json.NewDecoder(file)	
				for {
					var kv KeyValue
					if err = dec.Decode(&kv); err != nil {
						break
					}
					m[kv.Key] = append(m[kv.Key], kv.Value)
				}
				arg.FinishedMapFile[mid] = 1
			} else {
				log.Fatalf("Error after reduce request")
			}
		}
		arg.RequestFile = nil
	}

	oname := "mr-out-"
	oname += strconv.Itoa(arg.Id)
	ofile, _ := os.Create(oname)
	for k, v := range m {
		res := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, res)
	}
	ofile.Close()
	call("Master.ReduceFinish", arg, arg)
	atomic.AddInt32(&curReduceThreadNum, -1)
}

//
// main/mrworker.go calls this function:
//   mapf, reducef := loadPlugin(os.Args[1])
//   mr.Worker(mapf, reducef)
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	curMapThreadNum = 0
	curReduceThreadNum = 0
	loop := true

	for loop {
		var worker WorkerStruct
		for {
			err := call("Master.RequestTask", 0, &worker)
			if fmt.Sprintf("%v", err) == fmt.Sprintf("%v", ErrDone) {
				printf("break: %v\n", err)
				loop = false
				break
			} else if fmt.Sprintf("%v", err) == fmt.Sprintf("%v", ErrNoMoreTask) {
				printf("sleep: %v\n", err)
				time.Sleep(100 * time.Millisecond)
			} else {
				break
			}
		}

		if !loop {
			break
		}

		if worker.Type == WORKER_TYPE_MAP {
			for atomic.LoadInt32(&curMapThreadNum) == maxMapThreadNum {
				time.Sleep(100 * time.Millisecond)
			}
			atomic.AddInt32(&curMapThreadNum, 1)
			printf("MapObj: %v\n", worker.MapObj)
			go MapFunc(&worker.MapObj, mapf)
		} else {
			for atomic.LoadInt32(&curReduceThreadNum) == maxReduceThreadNum {
				time.Sleep(100 * time.Millisecond)
			}
			atomic.AddInt32(&curReduceThreadNum, 1)
			printf("ReduceObj: %v\n", worker.ReduceObj)
			go ReduceFunc(&worker.ReduceObj, reducef)
		}
	}

	// to be foolproof
	for atomic.LoadInt32(&curMapThreadNum) > 0 || atomic.LoadInt32(&curReduceThreadNum) > 0{
		time.Sleep(100 * time.Millisecond)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
