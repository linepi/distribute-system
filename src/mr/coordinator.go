package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"path/filepath"
	"time"
)

type WorkerState struct {
	Type int
	Done bool
	Assigned bool
	TimeVal time.Time
}

type Master struct {
	ReduceNum int
	MapNum int

	MapState []WorkerState // a bucket, MapState[i] means map i 
	ReduceState []WorkerState

	InputFiles []string
	IsDone bool

	mu sync.Mutex
}

func howmanydone(arr []WorkerState) int {
	num := 0
	for _, e := range arr {
		if e.Done {
			num++
		}
	}
	return num
}

func howmanyone(arr []int) int {
	num := 0
	for _, e := range arr {
		if e == 1 {
			num++
		}
	}
	return num
}

func firstnotassigned(arr []WorkerState) int {
	for i, e := range arr {
		if !e.Assigned {
			return i
		}
	}
	return -1
}

// Your code here -- RPC handlers for the worker to call.
// I found that the arg should used for read, and reply can only be written
func (m *Master) MapFinish(arg *Map, reply *Map) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MapState[arg.Id].Done = true
	return nil
}

func (m *Master) ReduceFinish(arg *Reduce, reply *Reduce) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ReduceState[arg.Id].Done = true
	if (howmanydone(m.ReduceState) == m.ReduceNum) {
		m.IsDone = true
	}
	return nil
}

func (m *Master) ReduceRequest(arg *Reduce, reply *Reduce) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if howmanyone(arg.FinishedMapFile) == m.MapNum {
		reply.Done = true
		return nil
	}
	// printf("reduce request: %v\n", arg)
	// printf("Reduce %v", arg.Id)
	// printf(" %v\n", arg.FinishedMapFile)
	for i := 0; i < m.MapNum; i++ {
		// printf("%d %d %d\n", len(m.MapState), len(arg.MapStateFile), i)
		if m.MapState[i].Done && arg.FinishedMapFile[i] == 0 {
			if reply.RequestFile == nil {
				reply.RequestFile = make([]int, 0)
			} 
			reply.RequestFile = append(reply.RequestFile, i)
		} else if m.MapState[i].Done && arg.FinishedMapFile[i] == 1 {
			log.Fatalf("some thing wrong")
		}
	}
	return nil
}

// allocate task to worker that requests task
// now take a naive method, that is: first map, then reduce
func (m *Master) RequestTask(_ int, worker *WorkerStruct) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.IsDone {
		printf("ErrDone\n")
		return ErrDone
	}

	for i := 0; i < m.MapNum; i++ {
		if m.MapState[i].Assigned && !m.MapState[i].Done {
			if time.Since(m.MapState[i].TimeVal) > 10 * time.Second {
				m.MapState[i].Assigned = false
			}
		}
	}
	for i := 0; i < m.ReduceNum; i++ {
		if m.ReduceState[i].Assigned && !m.ReduceState[i].Done {
			if time.Since(m.ReduceState[i].TimeVal) > 10 * time.Second {
				m.ReduceState[i].Assigned = false
			}
		}
	}

	if firstnotassigned(m.MapState) != -1 { 
		id := firstnotassigned(m.MapState)
		worker.Type = WORKER_TYPE_MAP
		worker.MapObj = Map{id, m.InputFiles[id]}
		printf("MapObj: %v\n", worker.MapObj)
		m.MapState[id].TimeVal = time.Now()
		m.MapState[id].Assigned = true
	} else if howmanydone(m.MapState) == m.MapNum && 
						firstnotassigned(m.ReduceState) != -1{ 
		// for simplicity, done all map work before launch reduce work
		id := firstnotassigned(m.ReduceState)
		worker.Type = WORKER_TYPE_REDUCE
		FinishedMapFile := make([]int, m.MapNum)
		for i := 0; i < m.MapNum; i++ {
			FinishedMapFile[i] = 0
		}
		worker.ReduceObj = Reduce{id, false, nil, FinishedMapFile}
		printf("ReduceObj: %v\n", worker.ReduceObj)
		m.ReduceState[id].TimeVal = time.Now()
		m.ReduceState[id].Assigned = true
	} else {
		// nothing to do
		// printf("ErrNoMoreTask\n")
		return ErrNoMoreTask
	}
	return nil
} 

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func Done(m *Master) bool {
	if m.IsDone {
		files, err := filepath.Glob(inter_file_prefix + "*")
		if err != nil {
			panic(err)
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				panic(err)
			}
		}
		return true
	} else {
		return false
	}
}

//
// create a Master.
// nReduce is the number of reduce tasks to use.
//
// main/mrmaster.go calls this function:
//   m := mr.MakeMaster(os.Args[1:], 10)
//   for m.Done() == false {
//   	 time.Sleep(time.Second)
//   }
//
func MakeCoordinator(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.MapNum = len(files)
	m.MapState = make([]WorkerState, m.MapNum)
	for i := 0; i < m.MapNum; i++ {
		m.MapState[i].Done = false
		m.MapState[i].Assigned = false
		m.MapState[i].Type = WORKER_TYPE_MAP
	}
	m.ReduceNum = nReduce
	m.ReduceState = make([]WorkerState, m.ReduceNum)
	for i := 0; i < m.ReduceNum; i++ {
		m.ReduceState[i].Done = false
		m.ReduceState[i].Assigned = false
		m.ReduceState[i].Type = WORKER_TYPE_REDUCE
	}

	m.InputFiles = files
	m.IsDone = false

	m.server()
	return &m
}
