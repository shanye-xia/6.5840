package mr

import (
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// declare an argument structure.
	for {
		//循环请求任务
		// fmt.Println("worker request task")
		args := TaskRequest{}
		reply := TaskResponse{}
		CallGetTask(&args, &reply)
		switch reply.Task.TaskType {
		case MapTask:
			//map任务
			DoMapTask(reply.Task, mapf)
			callDone(reply.Task)
		case ReduceTask:
			//reduce任务
			DoReduceTask(reply.Task, reducef)
			callDone(reply.Task)
		case WaitTask:
			//等待任务
			//休息一会再请求
			time.Sleep(time.Second * 2)
			// fmt.Println("worker wait a while")
		case ExitTask:
			//退出任务
			// fmt.Println("worker exit")
			return
		}
		// break
	}
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	// fmt.Println("worker do map task ", task.TaskId, " file name :", task.FileName)
	//读文件
	fileName := task.FileName
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	//中间结果暂存
	intermidate := mapf(fileName, string(content))
	reduceNum := task.ReduceNum

	//分桶
	//桶的数量就是reduce的数量, HashKv存放的是map的结果的数组，下标表示桶的编号
	HashKv := make([][]KeyValue, reduceNum)
	for _, kv := range intermidate {
		bucket := ihash(kv.Key) % reduceNum
		//把这个map结果放入对应的桶中方便reduce阶段处理
		HashKv[bucket] = append(HashKv[bucket], kv)
	}
	// // fmt.Printf("do map task %v, intermidate len %v\n", task.TaskId, len(intermidate))

	//把每个桶写入对应的中间文件
	for i := 0; i < reduceNum; i++ {
		mapOutPutFileName := fmt.Sprintf("mr-tmp-%v-%v", task.TaskId, i)
		mapOutPutFile, err := os.Create(mapOutPutFileName)
		if err != nil {
			log.Fatalf("cannot create %v", mapOutPutFileName)
		}
		enc := json.NewEncoder(mapOutPutFile)
		//遍历所有的kv对，写中间文件
		for _, kv := range HashKv[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		mapOutPutFile.Close()
		os.Rename(mapOutPutFileName, fmt.Sprintf("mr-%v-%v", task.TaskId, i))

	}

	// fmt.Printf("do map task %v done\n", task.TaskId)

}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	// fmt.Println("worker do reduce task ", task.TaskId)
	ReduceFileNum := task.TaskId - 1000
	//获取这个reduce任务需要处理的中间文件
	files := task.ReduceFiles
	// fmt.Printf("reduce task %v get files %v\n", task.TaskId, files)
	//读取文件得到所有的intermediate
	var intermediate []KeyValue
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}
	// fmt.Printf("reduce task %v read all files done, kvs len %v\n", task.TaskId, len(intermediate))

	//对kvs进行排序
	shuffle(intermediate)

	//创建输出文件
	reduceOutPutFileName := fmt.Sprintf("mr-tmp-%v", ReduceFileNum)
	reduceOutPutFile, err := os.Create(reduceOutPutFileName)
	if err != nil {
		log.Fatalf("cannot create %v", reduceOutPutFileName)
	}

	//进行count
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		//此时 [i,j) 的key都一样
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// // fmt.Printf("reduce task %v reduce key %v values %v output %v\n", task.TaskId, intermediate[i].Key, values, output)
		//写入输出文件
		fmt.Fprintf(reduceOutPutFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	reduceOutPutFile.Close()
	//重命名输出文件
	os.Rename(reduceOutPutFileName, fmt.Sprintf("mr-out-%v", ReduceFileNum))
	// fmt.Printf("do reduce task %v done\n", ReduceFileNum)
}



func shuffle(kvs []KeyValue) {
	//对kvs进行排序
	// 按key排序
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})
	// fmt.Printf("after sort kvs len %v\n", len(kvs))
}

func callDone(Task *Task) {
	//完成了自己的任务
	// fmt.Println("worker call done for task ", Task.TaskId)
	args := TaskDoneRequest{
		TaskId:   Task.TaskId,
		TaskType: Task.TaskType,
	}
	reply := TaskDoneResponse{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok && reply.Ack {
		// fmt.Println("worker call done success for task ", Task.TaskId)
	} else {
		// fmt.Println("worker call done failed for task ", Task.TaskId)
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
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
	}
}

func CallGetTask(args *TaskRequest, reply *TaskResponse) {

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// // fmt.Printf("Coordinator.GetTask call success!\n")
		// fmt.Printf("Coordinator.GetTask reply.Task %+v\n", *reply.Task)
		// // fmt.Printf("Coordinator.GetTask reply.Task.id %v reply.Task.type %v\n", reply.Task.TaskId,reply.Task.TaskType)
	} else {
		// fmt.Printf("Coordinator.GetTask call failed!\n")
	}
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

	// fmt.Println(err)
	return false
}
