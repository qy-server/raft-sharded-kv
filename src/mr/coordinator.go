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

// coordinator.go
type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type Task struct {
	ID        int
	FileName  string
	Status    TaskStatus
	StartTime time.Time //开始时间是什么意思？
}

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	phase Phase

	mapTasks    []Task
	reduceTasks []Task

	nMap    int
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   MapPhase,
		nMap:    len(files),
		nReduce: nReduce,
	}
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			ID:       i,
			FileName: file,
			Status:   Idle,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			ID:     i,
			Status: Idle,
		})
	}

	c.server(sockname)
	return &c
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if c.phase == MapPhase {
		if c.allMapTasksDone() {
			c.phase = ReducePhase
		} else {
			for i := range c.mapTasks {
				if c.mapTasks[i].Status == Idle || c.taskTimedOut(c.mapTasks[i]) { //timeout怎么断定的？
					c.mapTasks[i].Status = InProgress
					c.mapTasks[i].StartTime = time.Now()

					reply.TaskType = MapTask
					reply.TaskID = c.mapTasks[i].ID
					reply.FileName = c.mapTasks[i].FileName
					return nil
				}
			}

			reply.TaskType = WaitTask
			return nil
		}
	}

	if c.phase == ReducePhase {
		if c.allReduceTasksDone() {
			c.phase = DonePhase
			reply.TaskType = ExitTask
			return nil
		}

		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == Idle || c.taskTimedOut(c.reduceTasks[i]) {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskID = c.reduceTasks[i].ID
				return nil
			}
		}

		reply.TaskType = WaitTask
		return nil
	}

	reply.TaskType = ExitTask
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
			return nil
		} //为什么return nil

		if c.phase == MapPhase && c.mapTasks[args.TaskID].Status == InProgress {
			c.mapTasks[args.TaskID].Status = Completed
		}

		if c.phase == MapPhase && c.allMapTasksDone() {
			c.phase = ReducePhase
		}

	case ReduceTask:
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
			return nil
		}

		if c.phase == ReducePhase && c.reduceTasks[args.TaskID].Status == InProgress {
			c.reduceTasks[args.TaskID].Status = Completed
		}

		if c.phase == ReducePhase && c.allReduceTasksDone() {
			c.phase = DonePhase
		}
	}

	return nil
}

func (c *Coordinator) allMapTasksDone() bool {
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}
func (c *Coordinator) allReduceTasksDone() bool {
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) taskTimedOut(task Task) bool {
	return task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second
}
