package raftkv

import (
	"bytes"
	"encoding/gob"
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
	Kind  string
	Key   string
	Value string
	Id    int64
	Reqid int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db     map[string]string
	ack    map[int64]int
	result map[int]chan Op
}

func (kv *KVServer) AppendEntryToLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)

	if !isLeader {
		//log.Printf("@@@ Wrong Leader %d\n", kv.me)
		return false
	}
	//log.Printf("@@@ Found Leader, start to write \n")

	kv.mu.Lock()
	ch, ok := kv.result[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.result[index] = ch
	}
	kv.mu.Unlock()

	select {
	case op := <-ch:
		log.Printf("@@@ op is %v, entry is %v \n", op, entry)
		return op == entry
	case <-time.After(1000 * time.Millisecond):
		//log.Printf("@@@ come into timer iteration\n")
		return false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	entry := Op{
		Kind:  "Get",
		Key:   args.Key,
		Id:    args.Id,
		Reqid: args.ReqId}

	ok := kv.AppendEntryToLog(entry)

	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.ack[args.Id] = args.ReqId
		log.Printf("@@@+++ %d get:%v value:%s\n", kv.me, entry, reply.Value)
		kv.mu.Unlock()

	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	entry := Op{
		Kind:  args.Op,
		Key:   args.Key,
		Id:    args.Id,
		Reqid: args.ReqId,
		Value: args.Value,
	}
	ok := kv.AppendEntryToLog(entry)
	if !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
}

func (kv *KVServer) CheckDup(id int64, reqid int) bool {

	v, ok := kv.ack[id]
	if ok {
		return v >= reqid
	}
	return false
}

func (kv *KVServer) Apply(args Op) {
	switch args.Kind {
	case "Put":
		kv.db[args.Key] = args.Value
	case "Append":
		kv.db[args.Key] += args.Value
	}
	kv.ack[args.Id] = args.Reqid
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

func (kv *KVServer) applyMsg() {
	for {
		msg := <-kv.applyCh
		log.Printf("****** Get msg %v", msg)
		kv.rf.PrintInfo()
		if msg.UseSnapshot {

			// Here might have some bug
			r := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(r)

			log.Printf("****** Decode data from database buff = %v\n", r.Bytes())

			kv.mu.Lock()
			kv.db = make(map[string]string)
			kv.ack = make(map[int64]int)
			d.Decode(&kv.db)
			d.Decode(&kv.ack)
			kv.mu.Unlock()

		} else {
			op := msg.Command.(Op)
			kv.mu.Lock()
			if !kv.CheckDup(op.Id, op.Reqid) {
				kv.Apply(op)
			}
			ch, ok := kv.result[msg.CommandIndex]
			if ok {
				select {
				case <-kv.result[msg.CommandIndex]:
				default:
				}
				ch <- op
			} else {
				kv.result[msg.CommandIndex] = make(chan Op, 1)
			}

			if kv.maxraftstate != -1 && kv.rf.GetPersistSize() > kv.maxraftstate {
				tempBuff := new(bytes.Buffer)
				tempEncoder := gob.NewEncoder(tempBuff)
				tempEncoder.Encode(kv.db)
				tempEncoder.Encode(kv.ack)
				tempData := tempBuff.Bytes()

				log.Printf("****** Log data into database buff = %v\n", tempData)

				go kv.rf.StartSnapshot(tempData, msg.CommandIndex, kv.maxraftstate)
			}

			kv.mu.Unlock()
		}

	}

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
	kv.ack = make(map[int64]int)
	kv.result = make(map[int]chan Op)

	go kv.applyMsg()

	return kv
}
