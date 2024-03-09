package shardkv

import (
	"time"

	"6.5840/shardctrler"
)

func (kv *ShardKV) ConfigCommand(command RaftCommand, reply *OpReply) {
	index, _, isLeader := kv.rf.Start(command)

	// check if the server is the leader
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

func (kv *ShardKV) handleConfigChangeMessage(command RaftCommand) *OpReply {
	switch command.CmdType {
	case ConfigChange:
		newConfig := command.Data.(shardctrler.Config)
		return kv.applyNewConfig(newConfig)
	case ShardMigration:
		shardData := command.Data.(ShardOperationReply)
		return kv.applyShardMigration(&shardData)
	case ShardGC:
		shardData := command.Data.(ShardOperationArgs)
		return kv.applyShardGC(shardData)
	default:
		panic("unknown command type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	// check if the new config is the same as the current config
	if newConfig.Num == kv.currentConfig.Num+1 {
		for i := 0; i < shardctrler.NShards; i++ {
			// check if the shard is not in the current config, but in the new config
			// we need to let the shard in
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
				currentGid := kv.currentConfig.Shards[i]
				if currentGid != 0 {
					kv.shards[i].Status = MoveIn
				}
			}

			// todo check if the shard is in the current config, but is not in new config.
			// we need to let the shard out
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
				gid := newConfig.Shards[i]
				if gid != 0 {
					kv.shards[i].Status = MoveOut
				}
			}
		}

		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}

	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardMigration(shardDataReply *ShardOperationReply) *OpReply {
	if shardDataReply.ConfigNum == kv.currentConfig.Num {
		for shardId, shardData := range shardDataReply.ShardData {
			shard := kv.shards[shardId]
			if shard.Status == MoveIn {
				for k, v := range shardData {
					shard.KV[k] = v
				}
				// status change to gc
				shard.Status = GC
			} else {
				break
			}
		}
		// copy the duplicate table
		for clientId, dupTable := range shardDataReply.DuplicateTable {
			table, ok := kv.duplicateTable[clientId]
			if !ok || dupTable.SeqId > table.SeqId {
				kv.duplicateTable[clientId] = dupTable
			}
		}

	}
	return &OpReply{Err: ErrWrongConfig}
}

func (kv *ShardKV) applyShardGC(shardsInfo ShardOperationArgs) *OpReply {
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardsInfo.ShardIds {
			shard := kv.shards[shardId]
			if shard.Status == GC {
				shard.Status = Normal
			} else if shard.Status == MoveOut {
				kv.shards[shardId] = NewMemoryKVStateMachine()
			} else {
				break
			}
		}
	}

	return &OpReply{Err: ErrWrongConfig}
}
