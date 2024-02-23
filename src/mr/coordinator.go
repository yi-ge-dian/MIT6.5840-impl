package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex

// TaskStatus, [Idle, InProgress, Completed]
type TaskStatus int8

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

// TaskInfo, task information, maintained by the [coordinator]
type TaskInfo struct {
	// task status, Idle, InProgress, Completed
	TaskStatus

	// start time of the task
	StartTime time.Time

	// task reference for the task
	TaskReference *Task
}

type TaskType int8

const (
	Map TaskType = iota
	Reduce
	Exit
	Wait
)

type Task struct {
	// input file name, using for [map task]
	Input string

	// task type, Map or Reduce or Exit or Wait
	Type TaskType

	// number of reduce tasks
	NReducer int

	// task id
	TaskId int

	// intermediate file names, using for [reduce task]
	Intermediates []string
}

// State, [Map, Reduce, Exit, Wait]
type State = TaskType

type Coordinator struct {
	// task queue for task are not assigned
	TaskQueue chan *Task

	// task info for all tasks, [fileId ==> TaskInfo]
	AllTaskInfo map[int]*TaskInfo

	// current phase, [Map, Reduce, Exit, Wait]
	CurrentPhase State

	// Number of reduce tasks
	NReduce int

	// input file names
	InputFiles []string

	// intermediate file names, [0: [mr-0-0, mr-1-0, mr-2-0], 1: [mr-0-1, mr-1-1, mr-2-1], ...]
	Intermediates [][]string
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := c.CurrentPhase == Exit
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 0. create a coordinator
	c := Coordinator{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		AllTaskInfo:   make(map[int]*TaskInfo),
		CurrentPhase:  Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 1. split input files into 16MB-64MB files
	// 2. create map tasks
	c.createMapTasks()

	// 3. start a thread that listens for RPCs from worker.go
	c.server()

	// 4. start a goroutine to check timeout task
	go c.catchTimeOut()

	// 5. return the coordinator
	return &c
}

// catchTimeOut, checks if the task is timeout
func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()

		// if the current phase is Exit, return
		if c.CurrentPhase == Exit {
			mu.Unlock()
			return
		}

		// check if the task is timeout
		for _, taskInfo := range c.AllTaskInfo {
			if taskInfo.TaskStatus == InProgress && time.Since(taskInfo.StartTime) > 10*time.Second {
				c.TaskQueue <- taskInfo.TaskReference
				taskInfo.TaskStatus = Idle
				taskInfo.StartTime = time.Time{}
			}
		}
		mu.Unlock()
	}
}

// createMapTasks, creates map tasks and put them into the task queue
// and also initializes the task info
func (c *Coordinator) createMapTasks() {
	for fileId, fileName := range c.InputFiles {
		// create the map task
		task := Task{
			Input:    fileName,
			Type:     Map,
			NReducer: c.NReduce,
			TaskId:   fileId,
		}

		// put the task into the task queue
		c.TaskQueue <- &task

		// initialize the task info
		c.AllTaskInfo[fileId] = &TaskInfo{
			TaskStatus:    Idle,
			StartTime:     time.Time{},
			TaskReference: &task,
		}
	}
}

// createReduceTasks, creates reduce tasks and put them into the task queue
// and also initializes the task info
func (c *Coordinator) createReduceTasks() {
	// 1. clear the task queue
	c.AllTaskInfo = make(map[int]*TaskInfo)

	// 2. create reduce tasks
	for reduceTaskId, fileNames := range c.Intermediates {
		task := Task{
			Type:          Reduce,
			NReducer:      c.NReduce,
			TaskId:        reduceTaskId,
			Intermediates: fileNames,
		}

		// put the task into the task queue
		c.TaskQueue <- &task

		// initialize the task info
		c.AllTaskInfo[reduceTaskId] = &TaskInfo{
			TaskStatus:    Idle,
			StartTime:     time.Time{},
			TaskReference: &task,
		}
	}
}

// ********************************************************************************************************************
// RPCs
// ********************************************************************************************************************

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	mu.Lock()
	defer mu.Unlock()

	// check if the task queue is empty, if not, assign the task to the worker
	// for Map and Reduce phases
	if len(c.TaskQueue) > 0 && (c.CurrentPhase == Map || c.CurrentPhase == Reduce) {
		// get the task from the task queue
		task := *<-c.TaskQueue

		// update the task status
		c.AllTaskInfo[task.TaskId].TaskStatus = InProgress
		c.AllTaskInfo[task.TaskId].StartTime = time.Now()

		// assign the task to the worker
		*reply = AssignTaskReply{Task: task}

		return nil
	}

	if c.CurrentPhase == Exit {
		*reply = AssignTaskReply{Task: Task{Type: Exit}}
		return nil
	}

	*reply = AssignTaskReply{Task: Task{Type: Wait}}
	return nil
}

func (c *Coordinator) ProcessTaskFinished(args *ProcessTaskFinishedArgs, reply *ProcessTaskFinishedReply) error {
	mu.Lock()
	defer mu.Unlock()

	task := args.Task

	// old task or not the current phase
	if task.Type != c.CurrentPhase || c.AllTaskInfo[task.TaskId].TaskStatus == Completed {
		return nil
	}

	// update the task status
	c.AllTaskInfo[task.TaskId].TaskStatus = Completed

	// create a new goroutine to process the task result
	go c.processTaskResult(&task)
	return nil
}

// processTaskResult, processes the task result
func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()

	// according the state to do task
	switch task.Type {
	case Map:
		// get the intermediate file names, such as [mr-taskId-0, mr-taskId-1, mr-taskId-2]
		// and put them into the intermediates
		// so the result is [0: [mr-0-0, mr-1-0, mr-2-0], 1: [mr-0-1, mr-1-1, mr-2-1], ...]
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}

		// if all map tasks are done, create reduce tasks
		if c.allTaskDone() {
			c.createReduceTasks()
			c.CurrentPhase = Reduce
		}
	case Reduce:
		// if all reduce tasks are done, enter the exit phase
		if c.allTaskDone() {
			c.CurrentPhase = Exit
		}
	case Exit:
		// do nothing
	case Wait:
		// do nothing
	default:
		log.Fatalf("Unknown task state: %v", task.Type)
	}
}

// allTaskDone, checks if all tasks are done
func (c *Coordinator) allTaskDone() bool {
	for _, taskInfo := range c.AllTaskInfo {
		if taskInfo.TaskStatus != Completed {
			return false
		}
	}
	return true
}
