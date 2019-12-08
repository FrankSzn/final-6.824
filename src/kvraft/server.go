package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  string // "Get", "Put", "Append"
	Key   string
	Value string

	ClientId  int64 // 客户端id
	RequestId int   // 指令id
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here
	db     map[string]string
	result map[int]chan Op
	ack    map[int64]int
}

func (kv *KVServer) waitForResult(op Op, timeout time.Duration) bool {
	index, _, isLeader := kv.rf.Start(op)
	// DPrintf("index is %d", index)
	if isLeader == false {
		return true
	}

	var wrongLeader bool

	kv.mu.Lock()

	ch, ok := kv.result[index]
	// DPrintf("result is %v", ch)
	if !ok {
		// DPrintf("create a op")
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}

	kv.mu.Unlock()

	select {
	case entry := <-ch:
		kv.mu.Lock()
		wrongLeader = !kv.IsSameOp(entry, op)
		kv.mu.Unlock()

	case <-time.After(timeout):
		//DPrintf("timeout")
		kv.mu.Lock()
		if kv.IsDuplicateRequest(op.ClientId, op.RequestId) {
			wrongLeader = false
		} else {
			wrongLeader = true
		}
		kv.mu.Unlock()

	}
	kv.mu.Lock()
	delete(kv.result, index)
	kv.mu.Unlock()
	return wrongLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.CliendId,
		RequestId: args.RequestId,
	}

	reply.WrongLeader = kv.waitForResult(op, 500*time.Millisecond)

	if reply.WrongLeader == false {
		kv.mu.Lock()
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		if ok {
			reply.Value = value
			return
		}
		reply.Err = ErrNoKey
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	reply.WrongLeader = kv.waitForResult(op, 500*time.Millisecond)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//helper function
func (kv *KVServer) IsSameOp(a Op, b Op) bool {
	if a.ClientId == b.ClientId && a.Type == b.Type && a.Key == b.Key &&
		a.Value == b.Value && a.RequestId == b.RequestId {
		return true
	}
	return false
}

func (kv *KVServer) IsDuplicateRequest(clientId int64, requestId int) bool {
	appliedRequestId, ok := kv.ack[clientId]
	if ok == false || requestId > appliedRequestId {

		return false
	}

	return true
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.result = make(map[int]chan Op)
	kv.ack = make(map[int64]int)

	go func() {
		for msg := range kv.applyCh {
			if msg.CommandValid == false {
				continue
			}
			op := msg.Command.(Op)
			//DPrintf("server[%d]'s op is [%v]", kv.me, op)

			kv.mu.Lock()

			if kv.IsDuplicateRequest(op.ClientId, op.RequestId) {
				kv.mu.Unlock()
				continue
			}

			switch op.Type {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
			kv.ack[op.ClientId] = op.RequestId

			ch, ok := kv.result[msg.CommandIndex]
			if ok {
				ch <- op
			} else {

				kv.result[msg.CommandIndex] = make(chan Op, 1)
			}

			kv.mu.Unlock()

		}
	}()

	return kv
}
