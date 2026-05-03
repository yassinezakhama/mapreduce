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

type taskState int

const (
	idle taskState = iota
	inProgress
	done
)

type mapTask struct {
	id       int
	state    taskState
	fileName string
}

type reduceTask struct {
	id    int
	state taskState
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks []mapTask
	nRemMap  int

	reduceTasks []reduceTask
	nReduce     int
	nRemReduce  int
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nRemMap = len(files)
	c.mapTasks = make([]mapTask, len(files))
	for i, fname := range files {
		c.mapTasks[i] = mapTask{
			id:       i,
			fileName: fname,
			state:    idle,
		}
	}

	c.nRemReduce = nReduce
	c.nReduce = nReduce
	c.reduceTasks = make([]reduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = reduceTask{
			id:    i,
			state: idle,
		}
	}

	c.server()
	return &c
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nRemReduce != 0 {
		return false
	}
	if c.nRemMap != 0 {
		log.Fatal("invariant violated: reduce done but map not")
	}
	return true
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nRemMap > 0 {
		c.makeMapTask(args, reply)
		return nil
	}

	if c.nRemReduce > 0 {
		c.makeReduceTask(args, reply)
		return nil
	}

	for _, t := range c.reduceTasks {
		if t.state != done {
			reply.Type = Wait
			return nil
		}
	}

	reply.Type = Exit
	return nil
}

func (c *Coordinator) makeMapTask(_ *GetTaskArgs, reply *GetTaskReply) {
	for i := range c.mapTasks {
		if c.mapTasks[i].state != idle {
			continue
		}
		c.mapTasks[i].state = inProgress
		reply.Filename = c.mapTasks[i].fileName
		reply.TaskID = i
		reply.NReduce = c.nReduce
		reply.Type = Map
		go func(taskid int) {
			time.Sleep(time.Second * 10)

			c.mu.Lock()
			defer c.mu.Unlock()
			if c.mapTasks[taskid].state != done {
				c.mapTasks[taskid].state = idle
			}
		}(i)
		return
	}
	reply.Type = Wait
}

func (c *Coordinator) makeReduceTask(_ *GetTaskArgs, reply *GetTaskReply) {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].state != idle {
			continue
		}
		c.reduceTasks[i].state = inProgress
		reply.TaskID = i
		reply.NMap = len(c.mapTasks)
		reply.Type = Reduce
		go func(taskid int) {
			time.Sleep(time.Second * 10)

			c.mu.Lock()
			defer c.mu.Unlock()
			if c.reduceTasks[taskid].state != done {
				c.reduceTasks[taskid].state = idle
			}
		}(i)
		return
	}
	reply.Type = Wait
}

func (c *Coordinator) Report(args *ReportArgs, _ *ReportReply) error {
	taskid, tasktype := args.TaskID, args.TaskType

	c.mu.Lock()
	defer c.mu.Unlock()

	switch tasktype {
	case Map:
		if c.mapTasks[taskid].state == inProgress {
			c.nRemMap--
			c.mapTasks[taskid].state = done
		}
	case Reduce:
		if c.reduceTasks[taskid].state == inProgress {
			c.nRemReduce--
			c.reduceTasks[taskid].state = done
		}
	default:
		log.Fatalf("unexpected mr.TaskType: %#v", tasktype)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
