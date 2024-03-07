package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.
	configs        []Config // indexed by config num
	lastApplied    int
	stateMachine   *CtrlerStateMachine
	notifyChans    map[int]chan *OpReply
	duplicateTable map[int64]LastOperationInfo
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, seqId int64) bool {
	lastOpInfo, ok := sc.duplicateTable[clientId]
	return ok && seqId <= lastOpInfo.SeqId
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var opReply OpReply
	sc.command(&Op{
		OpType:   OpJoin,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Servers:  args.Servers,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(&Op{
		OpType:   OpLeave,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		GIDs:     args.GIDs,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var opReply OpReply
	sc.command(&Op{
		OpType:   OpMove,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Shard:    args.Shard,
		GID:      args.GID,
	}, &opReply)

	reply.Err = opReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var opReply OpReply
	sc.command(&Op{
		OpType: OpQuery,
		Num:    args.Num,
	}, &opReply)

	reply.Config = opReply.ControllerConfig
	reply.Err = opReply.Err
}

func (sc *ShardCtrler) command(args *Op, reply *OpReply) {
	sc.mu.Lock()
	if args.OpType != OpQuery && sc.isDuplicateRequest(args.ClientId, args.SeqId) {
		OpReply := sc.duplicateTable[args.ClientId].Reply
		reply.Err = OpReply.Err
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	notifyChan := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case res := <-notifyChan:
		reply.ControllerConfig = res.ControllerConfig
		reply.Err = res.Err
	case <-time.After(ClientRequestTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeNotifyChan(index)
		sc.mu.Unlock()
	}()

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.dead = 0
	sc.lastApplied = 0
	sc.stateMachine = NewCtrlerStateMachine()
	sc.notifyChans = make(map[int]chan *OpReply)
	sc.duplicateTable = make(map[int64]LastOperationInfo)

	go sc.applyTask()
	return sc
}

func (sc *ShardCtrler) applyTask() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				// if message is been proposed by the current server, ignore
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				// update the lastApplied index
				sc.lastApplied = msg.CommandIndex

				// apply the command to the state machine
				op := msg.Command.(Op)
				var opReply *OpReply

				if op.OpType != OpQuery && sc.isDuplicateRequest(op.ClientId, op.SeqId) {
					opReply = sc.duplicateTable[op.ClientId].Reply
				} else {
					opReply = sc.applyTaskToStateMachine(op)
					if op.OpType != OpQuery {
						sc.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
				}

				// send the result to the server
				if _, isLeader := sc.rf.GetState(); isLeader {
					notifyChan := sc.getNotifyChan(msg.CommandIndex)
					notifyChan <- opReply
				}

				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) applyTaskToStateMachine(op Op) *OpReply {
	var err Err
	var config Config

	switch op.OpType {
	case OpQuery:
		config, err = sc.stateMachine.Query(op.Num)
	case OpJoin:
		err = sc.stateMachine.Join(op.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(op.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(op.Shard, op.GID)
	default:
		panic("Unknown operation type")
	}
	return &OpReply{
		ControllerConfig: config,
		Err:              err,
	}
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *OpReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeNotifyChan(index int) {
	delete(sc.notifyChans, index)
}
