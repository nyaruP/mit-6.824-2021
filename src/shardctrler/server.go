package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	lastApplied    int
	stateMachine   ConfigStateMachine         // Configs数据一些列维护操作
	lastOperations map[int64]OperationContext //  客户端id最后的命令id和回复内容 （clientId，{最后的commdId，最后的LastReply}）
	notifyChans    map[int]chan *CommandReply // Leader回复给客户端的响应（日志Index， CommandReply
}

func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	//DPrintf("{server %v} has received %v", sc.me, args)
	sc.mu.RLock()
	if args.Op != OpQuery && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := sc.lastOperations[args.ClientId].LastReply
		reply.Err, reply.Config = lastReply.Err, lastReply.Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()

	// 不持有锁以提高吞吐量
	index, _, isLeader := sc.rf.Start(Command{args})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case res := <-ch:
		reply.Err, reply.Config = res.Err, res.Config
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := sc.lastOperations[clientId]
	return ok && commandId == operationContext.MaxAppliedCommandId
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for sc.killed() == false {
		select {
		case message := <-sc.applyCh:
			DPrintf("%v", message)
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				var reply *CommandReply
				command := message.Command.(Command)
				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = sc.lastOperations[command.ClientId].LastReply
				} else {
					reply = sc.applyLogToStateMachine(command)
					if command.Op != OpQuery {
						sc.lastOperations[command.ClientId] = OperationContext{command.CommandId, reply}
					}
				}

				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(message.CommandIndex)
					ch <- reply
				}
				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v: ", message))
			}

		}
	}
}
func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	var err Err
	var config Config
	switch command.Op {
	case OpQuery:
		config, err = sc.stateMachine.Query(command.Num)
	case OpMove:
		err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpJoin:
		err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(command.GIDs)
	}
	return &CommandReply{err, config}
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return sc.notifyChans[index]
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.stateMachine = NewMemoryConfigs()
	labgob.Register(Command{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastOperations = make(map[int64]OperationContext)
	sc.notifyChans = make(map[int]chan *CommandReply)
	sc.dead = 0
	sc.lastApplied = 0

	// 启动协程将日志提交给stateMachine
	go sc.applier()

	DPrintf("{ShardCtrler %v} has started", sc.rf.Me())
	return sc
}
