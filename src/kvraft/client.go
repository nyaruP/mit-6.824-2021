package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int64
	clientId  int64
	commandId int64 // (clientId, commandId) 保证操作序列唯一
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	return ck.Command(&CommandArgs{Key: key, Op: OpGet})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: OpPut})
	//ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.Command(&CommandArgs{Key: key, Value: value, Op: OpAppend})
	//ck.PutAppend(key, value, "Append")
}

//
// shared by Get, Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Command", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	// You will have to modify this function.
//}
func (ck *Clerk) Command(args *CommandArgs) string {
	args.CommandId, args.ClientId = ck.commandId, ck.clientId
	DPrintf("command is %v", args)
	for {
		var reply CommandReply
		if !ck.servers[ck.leaderId].Call("KVServer.Command", args, &reply) || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return reply.Value
	}
}
