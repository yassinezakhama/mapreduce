package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	Type     TaskType
	TaskID   int
	Filename string
	NReduce  int
	NMap     int
}

type ReportArgs struct {
	TaskType TaskType
	TaskID   int
}

type ReportReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
