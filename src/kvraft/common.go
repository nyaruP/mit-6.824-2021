package kvraft

import (
	"fmt"
	"log"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply           *CommandReply
}

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeout
)

func (err Err) String() string {
	switch err {
	case OK:
		return "Ok"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	// 手动触发宕机
	panic(fmt.Sprintf("unexpected Err %d", err))
}

type OperationOp uint8

const (
	OpGet OperationOp = iota
	OpPut
	OpAppend
)

func (op OperationOp) String() string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpAppend:
		return "OpAppend"
	case OpGet:
		return "OpGet"
	}
	panic(fmt.Sprintf("unexpected OperationOp %d", op))
}

type Command struct {
	*CommandArgs
}

type CommandArgs struct {
	Key       string
	Value     string
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

func (request CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v,Value:%v,Op:%v,ClientId:%v,CommandId:%v}", request.Key, request.Value, request.Op, request.ClientId, request.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (response CommandReply) String() string {
	return fmt.Sprintf("{Err:%v,Value:%v}", response.Err, response.Value)
}
