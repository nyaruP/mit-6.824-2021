package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

type Task struct {
	id           int
	genre        string
	mapInputFile string
	workerId     int
	deadLine     time.Time
}
type ApplyForTaskArgs struct {
	workerId      int
	lastTaskId    int
	lastTaskGenre string
}
type ApplyForTaskReply struct {
	taskId       int
	taskGenre    string
	mapInputFile string
	nReduce      int
	nMap         int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
func tmpMapOutFile(workerId int, mapId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-%d-%d", workerId, mapId, reduceId)
}

func finalMapOutFile(mapId int, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func tmpReduceOutFile(workerId int, reduceId int) string {
	return fmt.Sprintf("tmp-worker-%d-out-%d", workerId, reduceId)
}

func finalReduceOutFile(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
