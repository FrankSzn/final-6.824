package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// 3A
	clientId int64 // client id

	lastRequestId int // request instruction id, begin from 1, 初始化为0

	leaderHint int // last known leader id

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
	ck.clientId = nrand()
	ck.lastRequestId = 0
	ck.leaderHint = 0 // default server is 0
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
	requestId := ck.lastRequestId + 1
	var args GetArgs
	args.Key = key
	args.CliendId = ck.clientId
	args.RequestId = requestId

	for {
		var reply GetReply
		ok := ck.servers[ck.leaderHint].Call("KVServer.Get", &args, &reply)
		//DPrintf("Get return")
		if !ok || reply.WrongLeader == true {
			numOfservers := len(ck.servers)
			ck.leaderHint = (ck.leaderHint + 1) % numOfservers
			continue
		}

		ck.lastRequestId = requestId
		return reply.Value
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	requestId := ck.lastRequestId + 1
	var args PutAppendArgs
	args.ClientId = ck.clientId
	args.RequestId = requestId
	args.Op = op
	args.Key = key
	args.Value = value

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderHint].Call("KVServer.PutAppend", &args, &reply)

		if !ok || reply.WrongLeader == true {
			numOfServers := len(ck.servers)
			ck.leaderHint = (ck.leaderHint + 1) % numOfServers
			continue
		}
		ck.lastRequestId = requestId
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
