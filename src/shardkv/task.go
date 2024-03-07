package shardkv

import "time"

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
				op := msg.Command.(Op)
				var opReply *OpReply

				if op.OpType != OpGet && kv.isDuplicateRequest(op.ClientId, op.SeqId) {
					opReply = kv.duplicateTable[op.ClientId].Reply
				} else {
					opReply = kv.applyTaskToStateMachine(op)
					if op.OpType != OpGet {
						kv.duplicateTable[op.ClientId] = LastOperationInfo{
							SeqId: op.SeqId,
							Reply: opReply,
						}
					}
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
		kv.mu.Lock()
		newConfig := kv.mck.Query(-1)
		kv.currentConfig = newConfig
		kv.mu.Unlock()
		time.Sleep(FetchConfigInterval)
	}
}
