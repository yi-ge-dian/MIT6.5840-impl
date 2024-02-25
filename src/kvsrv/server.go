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

type PrevEntry struct {
	rpcId uint32
	value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// key ==> value
	kvs map[string]string

	// clientId ==> requestHistoryEntry
	prevEntries map[int64]PrevEntry
}

func (kv *KVServer) checkDuplicate(clientId int64, rpcId uint32) (string, bool) {
	// Your code here.
	prevEntry, ok := kv.prevEntries[clientId]

	if !ok || prevEntry.rpcId != rpcId {
		return "", false
	}

	return prevEntry.value, true
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

	clientId, rpcId := args.ClientId, args.RPCId
	prevVal, isDuplicate := kv.checkDuplicate(clientId, rpcId)

	if isDuplicate {
		reply.Value = prevVal
		return
	}

	kv.kvs[args.Key] = args.Value

	kv.prevEntries[clientId] = PrevEntry{
		rpcId: rpcId,
		value: "",
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientId, rpcId := args.ClientId, args.RPCId
	prevVal, isDuplicate := kv.checkDuplicate(clientId, rpcId)
	if isDuplicate {
		reply.Value = prevVal
		return
	}

	key, toAppendValue := args.Key, args.Value

	currVal, ok := kv.kvs[key]
	if ok {
		kv.kvs[key] = currVal + toAppendValue
		reply.Value = currVal
	} else {
		kv.kvs[key] = toAppendValue
		reply.Value = ""
	}

	kv.prevEntries[clientId] = PrevEntry{
		rpcId: rpcId,
		value: currVal,
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string, 0)
	kv.prevEntries = make(map[int64]PrevEntry, 0)

	return kv
}
