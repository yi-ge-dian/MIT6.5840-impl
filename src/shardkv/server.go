package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead        int32 // set by Kill()
	lastApplied int

	// shardId -> state machine
	shards         map[int]*MemoryKVStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
	currentConfig  shardctrler.Config
	prevConfig     shardctrler.Config
	mck            *shardctrler.Clerk
}

func (kv *ShardKV) isMatchGroup(key string) bool {
	shardId := key2shard(key)
	return kv.currentConfig.Shards[shardId] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// check request key is in the group
	kv.mu.Lock()
	if !kv.isMatchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data:    Op{Key: args.Key, OpType: OpGet},
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyChan := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case res := <-notifyChan:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChan(index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) isDuplicateRequest(clientId int64, seqId int64) bool {
	lastOpInfo, ok := kv.duplicateTable[clientId]
	return ok && seqId <= lastOpInfo.SeqId
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.isMatchGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// check if the request is repeated
	kv.mu.Lock()
	if kv.isDuplicateRequest(args.ClientId, args.SeqId) {
		OpReply := kv.duplicateTable[args.ClientId].Reply
		reply.Err = OpReply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// place the operation to the raft layer
	index, _, isLeader := kv.rf.Start(RaftCommand{
		CmdType: ClientOperation,
		Data: Op{
			Key:      args.Key,
			Value:    args.Value,
			OpType:   getOperationType(args.Op),
			ClientId: args.ClientId,
			SeqId:    args.SeqId,
		},
	})

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	notifyChan := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case res := <-notifyChan:
		reply.Err = res.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.dead = 0
	kv.lastApplied = 0
	kv.shards = make(map[int]*MemoryKVStateMachine)
	kv.notifyChans = make(map[int]chan *OpReply)
	kv.duplicateTable = make(map[int64]LastOperationInfo)
	kv.currentConfig = shardctrler.DefaultConfig()
	kv.prevConfig = shardctrler.DefaultConfig()

	// restore from snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())
	go kv.applyTask()
	go kv.fetchConfigTask()
	go kv.shardMigrationTask()
	go kv.shardGCTask()

	return kv
}

func (kv *ShardKV) applyTaskToStateMachine(op Op, shardId int) *OpReply {
	var value string
	var err Err
	switch op.OpType {
	case OpGet:
		value, err = kv.shards[shardId].Get(op.Key)
	case OpPut:
		err = kv.shards[shardId].Put(op.Key, op.Value)
	case OpAppend:
		err = kv.shards[shardId].Append(op.Key, op.Value)
	default:
		panic("unknown operation type")
	}
	return &OpReply{Value: value, Err: err}
}

func (kv *ShardKV) getNotifyChan(index int) chan *OpReply {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpReply, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) removeNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *ShardKV) makeSnapshot(index int) {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	enc.Encode(kv.shards)
	enc.Encode(kv.duplicateTable)
	kv.rf.Snapshot(index, buf.Bytes())
}

func (kv *ShardKV) restoreFromSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}

	buf := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(buf)
	var stateMachines map[int]*MemoryKVStateMachine
	var dupTable map[int64]LastOperationInfo
	if dec.Decode(&stateMachines) != nil || dec.Decode(&dupTable) != nil {
		panic("failed to restore state from snapshpt")
	}

	kv.shards = stateMachines
	kv.duplicateTable = dupTable
}
