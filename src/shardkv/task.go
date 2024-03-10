package shardkv

import (
	"sync"
	"time"
)

// applyTask
func (kv *ShardKV) applyTask() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				// if message is been proposed by the current server, ignore
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				// update the lastApplied index
				kv.lastApplied = msg.CommandIndex

				// apply the command to the state machine
				var opReply *OpReply
				raftCommand := msg.Command.(RaftCommand)

				if raftCommand.CmdType == ClientOperation { // check if the command is a client operation
					op := raftCommand.Data.(Op)
					opReply = kv.applyClientOperation(op)
				} else { // check if the command is a configuration change
					opReply = kv.handleConfigChangeMessage(raftCommand)
				}

				// send the result to the server
				if _, isLeader := kv.rf.GetState(); isLeader {
					notifyChan := kv.getNotifyChan(msg.CommandIndex)
					notifyChan <- opReply
				}

				// check if the raft state machine needs to be snapshot
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
					kv.makeSnapshot(msg.CommandIndex)
				}

				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				// apply the snapshot to the state machine
				kv.mu.Lock()
				kv.restoreFromSnapshot(msg.Snapshot)
				kv.lastApplied = msg.SnapshotIndex
				kv.mu.Unlock()
			}
		}
	}
}

// fetchConfigTask
// fetch the latest configuration from the shard controller
func (kv *ShardKV) fetchConfigTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			needFetch := true
			kv.mu.Lock()
			// if the status of a shard is non-normal, it means that a configuration change is in progress
			// and there is no need to obtain the configuration again, once process one
			for _, shard := range kv.shards {
				if shard.Status != Normal {
					needFetch = false
					break
				}
			}
			currentNum := kv.currentConfig.Num
			kv.mu.Unlock()

			if needFetch {
				// fetch the latest configuration
				newConfig := kv.mck.Query(currentNum + 1)
				if newConfig.Num == currentNum+1 {
					// send it to the raft layer
					kv.ConfigCommand(RaftCommand{CmdType: ConfigChange, Data: newConfig}, &OpReply{})
				}
			}
		}

		time.Sleep(FetchConfigInterval)
	}
}

// shardMigrationTask
func (kv *ShardKV) shardMigrationTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			// find the shard to move in
			gidToShards := kv.getShardByStatus(MoveIn)

			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					// Traverse each node in the Group and get the corresponding shard data from the leader
					shardArgs := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					for _, server := range servers {
						var reply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.GetShardData", &shardArgs, &reply)
						// get the shard data, do the shard migration
						if ok && reply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardMigration,
								Data:    reply,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardMigrationInterval)
	}
}

// shardGCTask
func (kv *ShardKV) shardGCTask() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.Lock()
			gidToShards := kv.getShardByStatus(GC)
			var wg sync.WaitGroup
			for gid, shardIds := range gidToShards {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					wg.Done()
					// Traverse each node in the Group and get the corresponding shard data from the leader
					shardArgs := ShardOperationArgs{
						ConfigNum: configNum,
						ShardIds:  shardIds,
					}
					for _, server := range servers {
						var reply ShardOperationReply
						clientEnd := kv.make_end(server)
						ok := clientEnd.Call("ShardKV.DeleteShardsData", &shardArgs, &reply)
						if ok && reply.Err == OK {
							kv.ConfigCommand(RaftCommand{
								CmdType: ShardGC,
								Data:    shardArgs,
							}, &OpReply{})
						}
					}
				}(kv.prevConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.Unlock()
			wg.Wait()
		}

		time.Sleep(ShardGCInterval)
	}
}

func (kv *ShardKV) getShardByStatus(status ShardStatus) map[int][]int {
	gidToShards := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.prevConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidToShards[gid]; !ok {
					gidToShards[gid] = make([]int, 0)
				}
				gidToShards[gid] = append(gidToShards[gid], i)
			}
		}
	}
	return gidToShards
}

func (kv *ShardKV) GetShardData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// current configuration is not need
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}

	// copy the shard data
	reply.ShardData = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.ShardData[shardId] = kv.shards[shardId].CopyData()
	}

	// copy the duplicate table
	reply.DuplicateTable = make(map[int64]LastOperationInfo)
	for clientId, lastOpInfo := range kv.duplicateTable {
		reply.DuplicateTable[clientId] = lastOpInfo.copyData()
	}

	reply.ConfigNum = kv.currentConfig.Num
	reply.Err = OK
}

func (kv *ShardKV) DeleteShardsData(args *ShardOperationArgs, reply *ShardOperationReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.currentConfig.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var opReply OpReply
	kv.ConfigCommand(RaftCommand{ShardGC, *args}, &opReply)

	reply.Err = opReply.Err
}

func (kv *ShardKV) applyClientOperation(op Op) *OpReply {
	if kv.isMatchGroup(op.Key) {
		var opReply *OpReply
		if op.OpType != OpGet && kv.isDuplicateRequest(op.ClientId, op.SeqId) {
			opReply = kv.duplicateTable[op.ClientId].Reply
		} else {
			shardId := key2shard(op.Key)
			opReply = kv.applyTaskToStateMachine(op, shardId)
			if op.OpType != OpGet {
				kv.duplicateTable[op.ClientId] = LastOperationInfo{
					SeqId: op.SeqId,
					Reply: opReply,
				}
			}
		}
		return opReply
	}
	return &OpReply{Err: ErrWrongGroup}
}
