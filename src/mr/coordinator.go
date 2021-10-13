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
	lock      sync.Mutex
	stage     string // 目前任务状态：MAP REDUCE DONE
	nMap      int
	nReduce   int
	tasks     map[string]Task
	toDoTasks chan Task
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
func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	// 记录woker的上一个任务完成
	if args.lastTaskId != -1 {
		c.lock.Lock()
		taskId := crateTaskId(args.lastTaskGenre, args.lastTaskId)
		if task, ok := c.tasks[taskId]; ok && task.workerId == args.workerId { // 加后一个条件原因是莫个work出现故障，要被回收
			log.Printf("%d 完成 %s-%d 任务", args.workerId, args.lastTaskGenre, args.lastTaskId)
			if args.lastTaskGenre == MAP {
				for i := 0; i < c.nReduce; i++ {
					err := os.Rename(
						tmpMapOutFile(args.workerId, args.lastTaskId, i),
						finalMapOutFile(args.lastTaskId, i))
					if err != nil {
						log.Fatalf(
							"Failed to mark map output file `%s` as final: %e",
							tmpMapOutFile(args.workerId, args.lastTaskId, i), err)
					}
				}
			} else if args.lastTaskGenre == REDUCE {
				err := os.Rename(
					tmpReduceOutFile(args.workerId, args.lastTaskId),
					finalReduceOutFile(args.lastTaskId))
				if err != nil {
					log.Fatalf(
						"Failed to mark reduce output file `%s` as final: %e",
						tmpReduceOutFile(args.workerId, args.lastTaskId), err)
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
	// 更新task
	task.workerId = args.workerId
	task.deadLine = time.Now().Add(10 * time.Second)
	c.tasks[crateTaskId(task.genre, task.id)] = task
	// 给work返回数据
	reply.taskId = task.id
	reply.taskGenre = task.genre
	reply.mapInputFile = task.mapInputFile
	reply.nMap = c.nMap
	reply.nReduce = c.nReduce
	return nil
}

func (c *Coordinator) cutover() {
	if c.stage == MAP {
		log.Printf("所有的MAP任务已经完成！")
		c.stage = REDUCE
		for i := 0; i < c.nReduce; i++ {
			task := Task{id: i, genre: REDUCE, workerId: -1}
			c.tasks[crateTaskId(task.genre, i)] = task
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
// nReduce is the number of reduce tasks to use.
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
			id:           i,
			genre:        "MAP",
			workerId:     -1,
			mapInputFile: file,
		}
		c.tasks[crateTaskId(task.genre, task.id)] = task
		c.toDoTasks <- task
	}

	c.server()

	// 多线程启动回收机制
	go func() {
		time.Sleep(500 * time.Millisecond)
		c.lock.Unlock()
		for _, task := range c.tasks {
			if task.workerId != -1 && time.Now().After(task.deadLine) {
				log.Printf("%d 运行任务 %s %d 出现故障，重新收回！", task.workerId, task.genre, task.id)
				task.workerId = -1
				c.toDoTasks <- task
			}
		}
		c.lock.Unlock()
	}()
	return &c
}
func crateTaskId(genre string, id int) string {
	return fmt.Sprintf("%s-%d", genre, id)
}
