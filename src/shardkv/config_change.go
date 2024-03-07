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
	default:
		panic("unknown command type")
	}
}

func (kv *ShardKV) applyNewConfig(newConfig shardctrler.Config) *OpReply {
	// check if the new config is the same as the current config
	if newConfig.Num == kv.currentConfig.Num+1 {
		for i := 0; i < shardctrler.NShards; i++ {
			// todo check if the shard is not in the current config, but in the new config
			// we need to let the shard in, 
			if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {

			}

			// todo check if the shard is in the current config, but is not in new config.
			// we need to let the shard out, todo
			if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {

			}
		}

		kv.currentConfig = newConfig
		return &OpReply{Err: OK}
	}

	return &OpReply{Err: ErrWrongConfig}
}
