package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const ExecuteTimeout = 500 * time.Millisecond

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied    int                        //
	stateMachine   KVStateMachine             // 服务器数据存储（key，value）
	lastOperations map[int64]OperationContext // 客户端id最后的命令id和回复内容 （clientId，{最后的commdId，最后的LastReply}）
	notifyChans    map[int]chan *CommandReply // Leader回复给客户端的响应（日志Index， CommandReply）
}

// 处理并返回客户端结果
func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), args, reply)
	kv.mu.RLock()
	if args.Op == OpGet && kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Err, reply.Value = lastReply.Err, lastReply.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case res := <-ch:
		reply.Err, reply.Value = res.Err, res.Value
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

//func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
//	// Your code here.
//}
//
//func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply *CommandReply
				command := message.Command.(Command)
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					reply = kv.lastOperations[command.ClientId].LastReply
				} else {
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{command.CommandId, reply}
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- reply
				}
				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
					kv.restoreSnapshot(message.Snapshot)
					kv.lastApplied = message.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}

		}
	}
}

//每个RPC都意味着客户端已经看到了它之前的RPC的回复。
//因此，我们只需要判断一个clientId的最新commandId是否满足条件
func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := kv.lastOperations[clientId]
	return ok && commandId <= operationContext.MaxAppliedCommandId
}

func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	var value string
	var err Err
	switch command.Op {
	case OpGet:
		value, err = kv.stateMachine.Get(command.Key)
	case OpPut:
		err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		err = kv.stateMachine.Append(command.Key, command.Value)
	}
	return &CommandReply{err, value}
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChans[index]
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dead = 0
	kv.lastApplied = 0
	kv.stateMachine = NewMemoryKV()
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandReply)

	// You may need initialization code here.
	kv.restoreSnapshot(persister.ReadSnapshot())
	go kv.applier()

	DPrintf("{Node %v} has started", kv.rf.Me())
	return kv
}

// 持久化
func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]OperationContext
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
	}
	kv.stateMachine, kv.lastOperations = &stateMachine, lastOperations
}
func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	kv.rf.Snapshot(index, w.Bytes())
}
