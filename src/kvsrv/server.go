package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	rid     int
	content string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	mem      map[string]string
	dupTable map[int64]Entry
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exist := kv.mem[args.Key]
	if exist {
		reply.Value = val
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.mem[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, empty := kv.dupTable[args.ClientId]; !empty {
		kv.dupTable[args.ClientId] = Entry{-1, ""}
	}

	entry := kv.dupTable[args.ClientId]

	if entry.rid == args.Rid {
		reply.Value = entry.content
		return
	}

	entry.rid = args.Rid
	entry.content = kv.mem[args.Key]
	kv.dupTable[args.ClientId] = entry // important, entry is just a copy of kv.dupTable[args.ClientId]

	// bad case: if we don't renew kv.dupTable[args.ClientId] here, assmue we have a Append request
	// try to append 5 in 1 2 3 4, and we did append it, so the mem has 1 2 3 4 5 and entry.content is 1 2 3 4.
	// But this reply doesn't make it to client, so client detect that, resend a append request
	// In this code, logic is fine. But entry.rid is always -1 because we never update it.
	// So code snippet "if entry.rid == args.Rid" will never be executed.
	// at the same time, last time we have update 1 2 3 4 to 1 2 3 4 5, and we should have execute if branch
	// and reply.Value will return 1 2 3 4(because entry.content remains unchanged). But we skip,
	// we renew the entry.content to 1 2 3 4 5, so it cause error.

	reply.Value = entry.content
	kv.mem[args.Key] = kv.mem[args.Key] + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.

	// initialize mem map and duplicate table
	kv.mem = make(map[string]string)
	kv.dupTable = make(map[int64]Entry)
	return kv
}
