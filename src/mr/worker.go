package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 单机运行，直接使用 PID 作为 Worker ID，方便 debug
	id := os.Getpid()
	log.Printf("Worker %d 开始工作：\n", id)

	lastTaskId := -1
	lastTaskType := ""
	for {
		args := ApplyForTaskArgs{
			WorkerId:     id,
			LastTaskId:   lastTaskId,
			LastTaskType: lastTaskType,
		}
		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)
		switch reply.TaskType {
		case "":
			log.Printf("接收到所有任务完成信号！")
			goto End
		case MAP:
			doMapTask(id, reply.TaskId, reply.MapInputFile, reply.NReduce, mapf)
		case REDUCE:
			doReduceTask(id, reply.TaskId, reply.NMap, reducef)
		}
		lastTaskId = reply.TaskId
		lastTaskType = reply.TaskType
		log.Printf("完成 %s 任务 %d", reply.TaskType, reply.TaskId)
	}
End:
	log.Printf("Worker %d 结束工作\n", id)

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doMapTask(id int, taskId int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	// 读入输入数据
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("%s 文件打开失败！", fileName)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("%s 文件内容读取失败！", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	hashedKva := make(map[int][]KeyValue)
	for _, kv := range kva {
		hashed := ihash(kv.Key) % nReduce
		hashedKva[hashed] = append(hashedKva[hashed], kv)
	}

	for i := 0; i < nReduce; i++ {
		outFile, _ := os.Create(tmpMapOutFile(id, taskId, i))
		for _, kv := range hashedKva[i] {
			fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
		}
		outFile.Close()
	}
}

func doReduceTask(id int, taskId int, nMap int, reducef func(string, []string) string) {
	var lines []string
	for i := 0; i < nMap; i++ {
		file, err := os.Open(finalMapOutFile(i, taskId))
		if err != nil {
			log.Fatalf("文件 %s 打开失败！", finalMapOutFile(i, taskId))
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("文件 %s 读取失败！", finalMapOutFile(i, taskId))
		}
		lines = append(lines, strings.Split(string(content), "\n")...)
	}

	var kva []KeyValue
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		split := strings.Split(line, "\t")
		kva = append(kva, KeyValue{
			Key:   split[0],
			Value: split[1],
		})
	}

	sort.Sort(ByKey(kva))

	outFile, _ := os.Create(tmpReduceOutFile(id, taskId))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
