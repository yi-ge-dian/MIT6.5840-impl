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

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// key ==> value
	kvs map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.kvs[args.Key]

	if !ok {
		val = ""
	}

	reply.Value = val
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvs[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	toAppendValue := args.Value

	currVal, ok := kv.kvs[key]
	if ok {
		kv.kvs[key] = currVal + toAppendValue
		reply.Value = currVal
	} else {
		kv.kvs[key] = toAppendValue
		reply.Value = ""
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string, 0)

	return kv
}
