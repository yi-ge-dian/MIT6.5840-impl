package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		// get task from coordinator
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		call("Coordinator.AssignTask", &args, &reply)
		task := reply.Task

		// according the state to do task
		switch task.Type {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(1 * time.Second)
		case Exit:
			time.Sleep(1 * time.Second)
			return
		default:
			log.Fatalf("Unknown task type: %v", reply.Task.Type)
		}
	}
}

// mapper function
func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// 1. read file
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}

	// 2. call map function to get intermediates
	// such as [A -> 1, B -> 1, C -> 1]
	intermediates := mapf(task.Input, string(content))

	// 3. the cached results are written to the local disk and cut into R slots
	buffer := make([][]KeyValue, task.NReducer)
	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.NReducer
		buffer[slot] = append(buffer[slot], intermediate)
	}
	outMapFileName := make([]string, 0)
	for i := 0; i < task.NReducer; i++ {
		// taskId is the mr-x-y ' x
		// i is the mr-x-y ' y
		// such as mr-taskId-0, mr-taskId-1, mr-taskId-2
		outMapFileName = append(outMapFileName, writeToLocalFile(task.TaskId, i, &buffer[i]))
	}

	// 4. send the location of R files to the coordinator.
	// such as [mr-taskId-0, mr-taskId-1, mr-taskId-2]
	task.Intermediates = outMapFileName

	// 5. send the task finish signal to the coordinator
	args := ProcessTaskFinishedArgs{
		Task: *task,
	}
	reply := ProcessTaskFinishedReply{}
	call("Coordinator.ProcessTaskFinished", &args, &reply)
}

// reducer function
func reducer(task *Task, reducef func(string, []string) string) {
	// 1. read intermediate files from local disk, such as [mr-0-0, mr-1-0, mr-2-0]
	intermediate := *readFromLocalFile(task.Intermediates)

	// 2. sort the intermediate key/value pairs by key
	sort.Sort(ByKey(intermediate))

	// 3. call reduce function to get result
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
		// put the same key together, group and merge
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// hand it over to reducef and get the result.
		output := reducef(intermediate[i].Key, values)

		// write to the corresponding output file
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), oname)

	args := ProcessTaskFinishedArgs{
		Task: *task,
	}
	reply := ProcessTaskFinishedReply{}
	call("Coordinator.ProcessTaskFinished", &args, &reply)
}

// writeToLocalFile
// write the intermediate key/value pairs to local file
func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

// readFromLocalFile
// read the intermediate key/value pairs from local file, such as [mr-0-0, mr-1-0, mr-2-0]
func readFromLocalFile(files []string) *[]KeyValue {
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
