package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

func reportTask(taskType TaskType, taskID int) {
	args := ReportTaskArgs{
		TaskType: taskType,
		TaskID:   taskID,
	}
	reply := ReportTaskReply{}

	call("Coordinator.ReportTask", &args, &reply)
}

func doMapTask(task RequestTaskReply, mapf func(string, string) []KeyValue) {
	//读取文件内容
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("cannot read %v: %v", task.FileName, err)
	}
	//转换键值对格式
	kva := mapf(task.FileName, string(content))
	//创建 Reduce 分区
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceID, bucket := range buckets {
		// Write to a temporary file first so a crash cannot leave a half-written
		// mr-X-Y file for a reducer to consume.
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceID)
		tmpFile, err := os.CreateTemp(".", finalName+"-*")
		if err != nil {
			log.Fatalf("cannot create temp intermediate file: %v", err)
		}

		writer := bufio.NewWriter(tmpFile)
		enc := json.NewEncoder(writer)
		for _, kv := range bucket {
			if err := enc.Encode(&kv); err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				log.Fatalf("cannot encode intermediate key/value: %v", err)
			}
		}

		if err := writer.Flush(); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			log.Fatalf("cannot flush intermediate file: %v", err)
		}
		if err := tmpFile.Close(); err != nil {
			os.Remove(tmpFile.Name())
			log.Fatalf("cannot close temp intermediate file: %v", err)
		}
		if err := os.Rename(tmpFile.Name(), finalName); err != nil {
			os.Remove(tmpFile.Name())
			log.Fatalf("cannot rename %v to %v: %v", tmpFile.Name(), finalName, err)
		}
	}
}

func doReduceTask(task RequestTaskReply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	for mapID := 0; mapID < task.NMap; mapID++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapID, task.TaskID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v: %v", fileName, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				file.Close()
				log.Fatalf("cannot decode intermediate file %v: %v", fileName, err)
			}
			intermediate = append(intermediate, kv)
		}

		if err := file.Close(); err != nil {
			log.Fatalf("cannot close intermediate file %v: %v", fileName, err)
		}
	}

	sort.Sort(ByKey(intermediate))

	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)
	tmpFile, err := os.CreateTemp(".", finalName+"-*")
	if err != nil {
		log.Fatalf("cannot create temp output file: %v", err)
	}
	writer := bufio.NewWriter(tmpFile)

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		if _, err := fmt.Fprintf(writer, "%v %v\n", intermediate[i].Key, output); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			log.Fatalf("cannot write reduce output: %v", err)
		}

		i = j
	}

	if err := writer.Flush(); err != nil {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
		log.Fatalf("cannot flush reduce output: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpFile.Name())
		log.Fatalf("cannot close temp output file: %v", err)
	}
	if err := os.Rename(tmpFile.Name(), finalName); err != nil {
		os.Remove(tmpFile.Name())
		log.Fatalf("cannot rename %v to %v: %v", tmpFile.Name(), finalName, err)
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		switch reply.TaskType {
		case MapTask:
			doMapTask(reply, mapf)
			reportTask(MapTask, reply.TaskID)

		case ReduceTask:
			doReduceTask(reply, reducef)
			reportTask(ReduceTask, reply.TaskID)

		case WaitTask:
			time.Sleep(time.Second)

		case ExitTask:
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
