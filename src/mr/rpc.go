package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType TaskType

	TaskID   int
	FileName string

	NReduce int
	NMap    int
}

type ReportTaskArgs struct {
	TaskType TaskType
	TaskID   int
}

type ReportTaskReply struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
