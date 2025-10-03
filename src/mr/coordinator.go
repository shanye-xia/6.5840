package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	mu             sync.Mutex // 互斥锁，保护共享数据
	State          int        // 0 for map, 1 for reduce,2 for done
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
	TaskType    TaskType // 0 for map 1 for reduce 2 for wait 3 for exit
	FileName    string
	TaskId      int
	ReduceFiles []string // map任务产生的中间文件名列表，reduce任务需要用到
	ReduceNum   int      // Reduce的数量
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
				task, ok := <-c.MapTaskChan
				if !ok {
					//chan 已关闭，且无更多数据
					fmt.Println("chan 已关闭，且无更多数据")
					reply.Task = &Task{
						TaskType: ExitTask,
						TaskId:   -3,
						FileName: "ExitTask",
					}
				} else {
					reply.Task = task
				}

			}
		}
	case ReducePhase:
		{
			//reduce阶段
			//先判断一下有没有任务
			if len(c.ReduceTaskChan) == 0 { //没有任务了，判断一下任务是否都完成了
				phaseDone := c.taskHandler.cheakPhaseDone()
				if phaseDone {
					//任务完成了，进入下一个阶段，Done
					c.nextPhase()
					//通知worker任务完成，退出
					reply.Task = &Task{
						TaskType: ExitTask,
						TaskId:   -3,
						FileName: "ExitTask",
					}
				} else {
					//任务还有没完成的，发送一个等待任务，状态为WaitTask，让worker等待一会在进行请求
					reply.Task = &Task{
						TaskType: WaitTask,
						TaskId:   -2,
						FileName: "WaitTask",
					}

				}
			} else {
				//还有任务没有分发，进行任务的分发
				task, ok := <-c.ReduceTaskChan
				if !ok {
					//chan 已关闭，且无更多数据
					fmt.Println("chan 已关闭，且无更多数据")
					reply.Task = &Task{
						TaskType: ExitTask,
						TaskId:   -3,
						FileName: "ExitTask",
					}
				} else {
					reply.Task = task
				}
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
	//保护共享数据
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("=================================================================")
	switch c.State {
	case MapPhase:
		//进入reduce阶段
		c.State = ReducePhase
		fmt.Println("all map tasks done, move to reduce phase")
		//初始化reduce任务队列
		c.MakeReduceTasks(c.ReduceNum)
	case ReducePhase:
		//进入done阶段
		c.State = DonePhase
		fmt.Println("all reduce tasks done, move to done phase")
	case DonePhase:
		//进入done阶段
		c.State = DonePhase
		fmt.Println("all tasks have already done!")
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
	c.mu.Lock()
	defer c.mu.Unlock()
	// Your code here.
	ret := false

	// Your code here.
	if c.State == DonePhase {
		ret = true
		fmt.Println("coordinator done!")
	}

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

	c.MakeMapTasks(files)

	fmt.Println("coordinator init success!")
	// Your code here.
	go c.crashDetector() //启动任务crash检测协程
	
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
	// close(c.MapTaskChan) //关闭map任务队列，防止重复添加
	fmt.Println("make map tasks done!")

}

func (c *Coordinator) MakeReduceTasks(nReduce int) {
	//初始化reduce任务队列
	fmt.Println("make reduce tasks")
	for i := 0; i < nReduce; i++ {
		task := Task{
			TaskType:    ReduceTask,
			ReduceFiles: getReduceFiles(i), //获取这个reduce任务需要处理的中间文件
			TaskId:      i + 1000,          // reduce任务id从1000开始
			ReduceNum:   c.ReduceNum,
		}

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
	// close(c.ReduceTaskChan) //关闭reduce任务队列，防止重复添加
	fmt.Println("make reduce tasks done!")
}

// 标记任务完成把TaskInfo的taskIsDone设为true
func (c *Coordinator) TaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	//保护共享数据
	c.mu.Lock()
	defer c.mu.Unlock()

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

func (c *Coordinator) crashDetector() {
	//定时检测任务是否crash了
	for {
		time.Sleep(time.Second * 2)
		c.mu.Lock()
		if c.State == DonePhase {
			c.mu.Unlock()
			break //任务完成了，退出
		}
		var timeout int64 = 10
		for _, taskInfo := range c.taskHandler.taskMap {
			task := taskInfo.Task
			//判断任务是否crash了
			crashed := c.taskHandler.cheakCrashedTask(task.TaskId, timeout)
			if crashed {
				//任务crash了，重新分发任务
				fmt.Printf("task %v crashed, reassigning\n", task.TaskId)
				switch task.TaskType {
				case MapTask:
					//重新放入map任务队列
					c.MapTaskChan <- task
					taskInfo.Task.TaskType = MapTask
					taskInfo.StartTime = time.Now().Unix() //更新开始时间
					taskInfo.taskIsDone = false            //重置任务状态
				case ReduceTask:
					//重新放入reduce任务队列
					c.ReduceTaskChan <- task
					taskInfo.Task.TaskType = ReduceTask
					taskInfo.StartTime = time.Now().Unix() //更新开始时间
					taskInfo.taskIsDone = false            //重置任务状态
				default:
					log.Fatalf("invalid task type %v", task.TaskType)
				}

			}
		}
		c.mu.Unlock()
	}
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

func (taskHandler *TaskHandler) cheakCrashedTask(taskid int, timeout int64) bool {
	//判断这个任务是否crash了
	taskInfo, ok := taskHandler.taskMap[taskid]
	if !ok {
		log.Fatalf("task %v not found, cannot cheak crashed", taskid)
		return false
	}
	if time.Now().Unix()-taskInfo.StartTime > timeout {
		//超过10秒没有完成，认为crash了
		return true
	} else {
		taskInfo.StartTime = time.Now().Unix() //更新开始时间
		//没有crash
		return false
	}

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
