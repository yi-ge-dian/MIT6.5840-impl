package shardctrler

import (
	"sort"
)

type CtrlerStateMachine struct {
	configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{
		configs: make([]Config, 1),
	}
	cf.configs[0] = DefaultConfig()
	return cf
}

// Query
// returns the configuration to which the client is querying.
func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(csm.configs) {
		return csm.configs[len(csm.configs)-1], OK
	}
	return csm.configs[num], OK
}

// Join
// adds a set of groups (gid -> server-list mapping).
// need to handle the workload balance issue.
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.configs)
	lastConfig := csm.configs[num-1]

	// construct the new configuration
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// add the new groups
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// re-assign the shards , gid -> shard
	// shard  gid
	// 0      1
	// 1      1
	// 2      2
	// 3      2
	// 4      1
	// after re-assign:
	// gid  shard
	// 1    0, 1, 4
	// 2    2, 3
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// rebalance the workload

	//  gid     shard
	//   1    [0, 1, 4, 8]
	//   2    [2, 3, 7]
	//   3    [5, 6, 9]
	//   4    []

	// The first iteration, the most shard is gid 1, and the least is the newly added gid 4, so move one to gid 4.
	//  gid     shard
	//   1    [1, 4, 8]
	//   2    [2, 3, 7]
	//   3    [5, 6, 9]
	//   4    [0]

	// The second iteration, the most shard is gid 1, and the least is gid 4, so move one to gid 4.
	//  gid     shard
	//   1    [4, 8]
	//   2    [2, 3, 7]
	//   3    [5, 6, 9]
	//   4    [0, 1]

	// The third iteration, the most shard is gid 2, the least is gid 1, the difference is equal to 1, so the end of the move, the cluster reaches balance
	for {
		maxGid := gidWithMaxShards(gidToShards)
		minGid := gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}

		// the minimum shard' s gid add one shard
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		// the maximum shard' s gid remove one shard
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// update the new configuration
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	csm.configs = append(csm.configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.configs)
	lastConfig := csm.configs[num-1]

	// construct the new configuration
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// re-assign the shards , gid -> shard
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// remove the gids
	var unassignedShards []int
	for _, gid := range gids {
		// delete the gid from group
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// retrive the shards from gid
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	// re-assign the unassigned shards
	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			minGid := gidWithMinShards(gidToShards)
			gidToShards[minGid] = append(gidToShards[minGid], shard)
		}

		for gid, shards := range gidToShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	csm.configs = append(csm.configs, newConfig)
	return OK
}

func (csm *CtrlerStateMachine) Move(shard int, gid int) Err {
	num := len(csm.configs)
	lastConfig := csm.configs[num-1]

	// construct the new configuration
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	newConfig.Shards[shard] = gid
	csm.configs = append(csm.configs, newConfig)

	return OK
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

// gidWithMaxShards
// returns the gid with the most shards, gid ==> shards
func gidWithMaxShards(gidToShards map[int][]int) int {
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}

	// to grantee that every node get the same config
	// we need to sort the map by gid
	// and then we can get the gid with the most shards

	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShards[gid])
		}
	}

	return maxGid
}

// gidWithMinShards
// returns the gid with the least shards, gid ==> shards
func gidWithMinShards(gidToShards map[int][]int) int {
	// to grantee that every node get the same config
	// we need to sort the map by gid
	// and then we can get the gid with the most shards

	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}

	return minGid
}
