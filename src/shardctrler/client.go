package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId  int64
	clientId  int64
	commandId int64
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
	// Your code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

// Query 查询最新的Config信息。
func (ck *Clerk) Query(num int) Config {
	return ck.Command(&CommandArgs{Num: num, Op: OpQuery})
}

// Join 新加入的Group信息
func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandArgs{Servers: servers, Op: OpJoin})
}

// Leave 哪些Group要离开。
func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandArgs{GIDs: gids, Op: OpLeave})
}

// Move 将Shard分配给GID的Group,无论它原来在哪。
func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandArgs{Shard: shard, GID: gid, Op: OpMove})
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	args.CommandId, args.ClientId = ck.commandId, ck.clientId
	DPrintf("ShardClient send %v, ", args)
	for {
		var reply CommandReply
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			//DPrintf("ShardClient receive %v", reply)
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return reply.Config
	}
}
