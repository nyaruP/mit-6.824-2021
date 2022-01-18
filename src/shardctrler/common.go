package shardctrler

import (
	"fmt"
	"log"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

const ExecuteTimeout = 500 * time.Millisecond

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid，分片位置信息，Shards[3]=2，说明分片序号为3的分片负贵的集群是Group2（gid=2）
	Groups map[int][]string // gid -> servers[],集群成员信息，Group[3]=['ip1','ip2'],说明gid = 3的集群Group3包含两台名称为ip1 & ip2的机器
}

type OperationOp uint8

const (
	OpJoin OperationOp = iota
	OpLeave
	OpMove
	OpQuery
)

func (op OperationOp) String() string {
	switch op {
	case OpJoin:
		return "OpJoin"
	case OpLeave:
		return "OpLeave"
	case OpMove:
		return "OpMove"
	case OpQuery:
		return "OpQuery"
	}
	panic(fmt.Sprintf("unexpected CommandOp %d", op))
}

type Command struct {
	*CommandArgs
}

type CommandArgs struct {
	Servers   map[int][]string // Join, new GID -> servers mappings
	GIDs      []int            // Leave
	Shard     int              // Move
	GID       int              // Move
	Num       int              // Query desired config number
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

type Err uint8

const (
	OK Err = iota
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "OK"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type CommandReply struct {
	Err    Err
	Config Config
}

type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
