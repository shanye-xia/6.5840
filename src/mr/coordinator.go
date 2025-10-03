package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Coordinator struct {
	State int // 0 for map, 1 for reduce, 2 for wait, 3 for exit
	MapTaskChan	chan* Task
	ReduceTaskChan chan* Task
	ReduceNum int	// Reduce的数量
	Files	[]string	// 文件名
}

type Task struct {
	TaskType int // 0 for map, 1 for reduce
	FileName string
	TaskId int
	ReduceNum int	// Reduce的数量
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask RPC handler.让worker获取任务
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	//判断当前状态
	if c.State == 0{
		//map阶段
		reply.Task = <- c.MapTaskChan
		// log.Printf("coordinator get task %+v", reply.Task)
	}else if c.State == 1{
		//从reduce任务队列中获取任务
	}
	
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化coordinator
	c := Coordinator{
		State: 0,
		MapTaskChan: make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		Files: files,
		ReduceNum: nReduce,
	}

	//初始化map任务队列
	for index, file := range c.Files {
		task := Task{
			TaskType: 0,
			FileName: file,
			TaskId: index,
			ReduceNum: c.ReduceNum,
			// ReduceNum: nReduce,
		}
		//放入map任务队列
		c.MapTaskChan <- &task
	}

	fmt.Println("coordinator init success!")
	// Your code here.


	c.server()
	return &c
}
