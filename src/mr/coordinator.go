package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	State          int // 0 for map, 1 for reduce,2 for done
	MapTaskChan    chan *Task
	ReduceTaskChan chan *Task
	ReduceNum      int         // Reduce的数量
	Files          []string    // 文件名
	taskHandler    TaskHandler // 用来存储所有的任务以便管理，key是taskId
}

// TaskHandler 用来管理任务的状态，有自己的方法
type TaskHandler struct {
	taskMap map[int]*Taskinfo // key是taskId
}

type Taskinfo struct {
	StartTime  int64
	taskIsDone bool
	Task       *Task
}

const (
	MapPhase = iota
	ReducePhase
	DonePhase
)

type Task struct {
	TaskType  TaskType // 0 for map 1 for reduce 2 for wait 3 for exit
	FileName  string
	TaskId    int
	ReduceFiles []string // map任务产生的中间文件名列表，reduce任务需要用到
	ReduceNum int // Reduce的数量
}

type TaskType int

// 枚举，增强可读性
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetTask RPC handler.让worker获取任务
func (c *Coordinator) GetTask(args *TaskRequest, reply *TaskResponse) error {
	//判断当前阶段
	switch c.State {
	case MapPhase:
		{
			//map阶段
			//先判断一下有没有任务
			if len(c.MapTaskChan) == 0 { //没有任务了，判断一下任务是否都完成了
				phaseDone := c.taskHandler.cheakPhaseDone()
				if phaseDone {
					//任务完成了，进入下一个阶段，Reduce
					c.nextPhase()
					fmt.Println("all map tasks done, move to reduce phase")
					//通知worker任务完成，退出
					reply.Task = &Task{
						TaskType: ExitTask,
						TaskId:   -3,
						FileName: "ExitTask",
					}
				} else {
					//任务还有没完成的，发送一个等待任务，状态为WaitTask，让worker等待一会在进行请求
					WaitTask := Task{
						TaskType: WaitTask,
						TaskId:   -2,
						FileName: "WaitTask",
					}
					reply.Task = &WaitTask

				}

				// log.Printf("coordinator get task %+v", reply.Task)
			} else {
				//还有任务没有分发，进行任务的分发
				reply.Task = <-c.MapTaskChan
			}
		}
	case ReducePhase:
		{
			//从reduce任务队列中获取任务，这里先模拟完成所有任务了
			reply.Task = &Task{
				TaskType: ExitTask,
				TaskId:   -3,
				FileName: "ExitTask",
			}
		}
	case DonePhase:
		{
			//任务完成了
			reply.Task = &Task{
				TaskType: ExitTask,
				TaskId:   -3,
				FileName: "ExitTask",
			}
		}

	}

	return nil
}

func (c *Coordinator) nextPhase() {
	switch c.State {
	case MapPhase:
		//进入reduce阶段
		c.State = ReducePhase
		fmt.Println("=================================================================")
	case ReducePhase, DonePhase:
		//进入done阶段
		c.State = DonePhase
		fmt.Println("all tasks done!")
	default:
		log.Fatalf("invalid state %v", c.State)
	}

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//初始化coordinator
	c := Coordinator{
		State:          0,
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
		Files:          files,
		ReduceNum:      nReduce,
		taskHandler: TaskHandler{
			taskMap: make(map[int]*Taskinfo, len(files)+nReduce), //长度应该是map任务数+reduce任务数
		},
	}

	// //初始化map任务队列
	// for index, file := range c.Files {
	// 	task := Task{
	// 		TaskType:  0,
	// 		FileName:  file,
	// 		TaskId:    index,
	// 		ReduceNum: c.ReduceNum,
	// 		// ReduceNum: nReduce,
	// 	}
	// 	//放入map任务队列
	// 	c.MapTaskChan <- &task
	// }
	c.MakeMapTasks(files)

	fmt.Println("coordinator init success!")
	// Your code here.

	c.server()
	return &c
}

func (c *Coordinator) MakeMapTasks(files []string) {
	//初始化map任务队列
	for index, file := range c.Files {
		task := Task{
			TaskType:  MapTask,
			FileName:  file,
			TaskId:    index,
			ReduceNum: c.ReduceNum,
		}

		Taskinfo := Taskinfo{
			StartTime:  time.Now().Unix(),
			Task:       &task,
			taskIsDone: false,
		}

		c.taskHandler.addTaskInfo(&Taskinfo)
		//放入map任务队列
		c.MapTaskChan <- &task
		fmt.Printf("make a map task : %+v\n", task)
	}

}

func (c *Coordinator) MakeReduceTasks(nReduce int) {
	//初始化reduce任务队列
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskType:  ReduceTask,
			ReduceFiles:  getReduceFiles(i), //获取这个reduce任务需要处理的中间文件
			TaskId:    i + 1000, // reduce任务id从1000开始
			ReduceNum: c.ReduceNum,
		}
		fmt.Printf("make a reduce task : %+v\n", task)

		Taskinfo := Taskinfo{
			StartTime:  time.Now().Unix(),
			Task:       &task,
			taskIsDone: false,
		}
		
		c.taskHandler.addTaskInfo(&Taskinfo)
		//放入reduce任务队列
		c.ReduceTaskChan <- &task
		fmt.Printf("make a reduce task : %+v\n", task)
	}
}

// 标记任务完成把TaskInfo的taskIsDone设为true
func (c *Coordinator) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	taskId := args.TaskId
	taskType := args.TaskType
	taskInfo, ok := c.taskHandler.taskMap[taskId]
	if !ok {
		log.Fatalf("task %v not found, cannot set done", taskId)
		reply.Ack = false
	} else {
		if taskInfo.taskIsDone {
			//已经标记完成
			reply.Ack = true
		}
		taskInfo.taskIsDone = true
		fmt.Printf("task %v of type %v marked as done\n", taskId, taskType)
		reply.Ack = true
	}
	return nil
}

// 初始化的时候添加任务信息
func (taskHandler *TaskHandler) addTaskInfo(Taskinfo *Taskinfo) {
	//防止重复添加
	taskId := Taskinfo.Task.TaskId
	task := taskHandler.taskMap[taskId]
	if task == nil {
		taskHandler.taskMap[taskId] = Taskinfo
	} else {
		log.Fatalf("task %v already exists", taskId)
	}
}

// 判断这个时期的所有任务完成了没有
func (taskHandler *TaskHandler) cheakPhaseDone() bool {
	//遍历taskMap，判断所有任务是否都完成
	for _, taskInfo := range taskHandler.taskMap {
		if !taskInfo.taskIsDone {
			return false
		}
	}
	return true
}

func getReduceFiles(reduceId int) []string {
	//获取这个reduce任务需要处理的中间文件
	//中间文件名格式：mr-tmp-X-Y  X是map任务id，Y是reduce任务id
	var files []string
	dir, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("cannot read current directory")
	}
	//前缀是mr-,后缀是-reduceId
	prefix := fmt.Sprintf("mr-")
	suffix := fmt.Sprintf("-%v", reduceId)
	//找到前缀是prefix，后缀是suffix的文件

	for _, file := range dir {
		name := file.Name()
		if len(name) > len(prefix)+len(suffix) && name[:len(prefix)] == prefix && name[len(name)-len(suffix):] == suffix {
			files = append(files, name)
		}
	}
	return files
}

