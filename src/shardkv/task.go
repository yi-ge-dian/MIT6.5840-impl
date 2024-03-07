package shardkv

import (
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
		// fetch the latest configuration
		kv.mu.Lock()
		newConfig := kv.mck.Query(kv.currentConfig.Num + 1)
		kv.mu.Unlock()

		// send it to the raft layer
		kv.ConfigCommand(RaftCommand{CmdType: ConfigChange, Data: newConfig}, &OpReply{})

		// update the current configuration
		kv.currentConfig = newConfig

		time.Sleep(FetchConfigInterval)
	}
}
