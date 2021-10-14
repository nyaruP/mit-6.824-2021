package mr

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	lock      sync.Mutex      // 锁
	stage     string          // 目前任务状态：MAP REDUCE DONE
	nMap      int             // MAP任务数量
	nReduce   int             // Reduce任务数量
	tasks     map[string]Task // 任务映射，主要是查看任务状态
	toDoTasks chan Task       // 待完成任务，采用通道实现，内置锁结构
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	log.Printf("Example")
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// 记录woker的上一个任务完成
	if args.LastTaskId != -1 {
		c.lock.Lock()
		taskId := crateTaskId(args.LastTaskType, args.LastTaskId)
		// 这里才产生最后的输出结果，是因为怕超时worker和合法worker都写入，造成冲突
		if task, ok := c.tasks[taskId]; ok && task.WorkerId == args.WorkerId { // 加后一个条件原因是莫个work出现故障，要被回收
			log.Printf("%d 完成 %s-%d 任务", args.WorkerId, args.LastTaskType, args.LastTaskId)
			if args.LastTaskType == MAP {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(
						tmpMapOutFile(args.WorkerId, args.LastTaskId, i),
						finalMapOutFile(args.LastTaskId, i))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.WorkerId, args.LastTaskId, i), err)
					}
				}
			} else if args.LastTaskType == REDUCE {
				err := os.Rename(
					tmpReduceOutFile(args.WorkerId, args.LastTaskId),
					finalReduceOutFile(args.LastTaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.WorkerId, args.LastTaskId), err)
				}
			}
			delete(c.tasks, taskId)
			if len(c.tasks) == 0 {
				c.cutover()
			}
		}
		c.lock.Unlock()
	}

	// 获取一个可用的Task并返回
	task, ok := <-c.toDoTasks
	// 通道关闭，代表整个MR作业已经完成，通知Work退出
	if !ok {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	log.Printf("Assign %s task %d to worker %dls"+
		"\n", task.Type, task.Id, args.WorkerId)
	// 更新task
	task.WorkerId = args.WorkerId
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[crateTaskId(task.Type, task.Id)] = task
	// 给work返回数据
	reply.TaskId = task.Id
	reply.TaskType = task.Type
	reply.MapInputFile = task.MapInputFile
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) cutover() {
	if c.stage == MAP {
		log.Printf("所有的MAP任务已经完成！开始REDUCE任务！")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{Id: i, Type: REDUCE, WorkerId: -1}
			c.tasks[crateTaskId(task.Type, i)] = task
			c.toDoTasks <- task
		}
	} else if c.stage == REDUCE {
		log.Printf("所有的REDUCE任务已经完成！")
		close(c.toDoTasks)
		c.stage = DONE
	}

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
	// Your code here.
	c.lock.Lock()
	ret := c.stage == DONE
	defer c.lock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		stage:     MAP,
		nMap:      len(files),
		nReduce:   nReduce,
		tasks:     make(map[string]Task),
		toDoTasks: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			Id:           i,
			Type:         MAP,
			WorkerId:     -1,
			MapInputFile: file,
		}
		log.Printf("Type: %s", task.Type)
		c.tasks[crateTaskId(task.Type, task.Id)] = task
		c.toDoTasks <- task
	}
	log.Printf("Coordinator start\n")
	c.server()

	// 多线程启动回收机制，回收超时任务
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != -1 && time.Now().After(task.DeadLine) {
					log.Printf("%d 运行任务 %s %d 出现故障，重新收回！", task.WorkerId, task.Type, task.Id)
					task.WorkerId = -1
					c.toDoTasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
func crateTaskId(genre string, id int) string {
	return fmt.Sprintf("%s-%d", genre, id)
}
