package shardctrler

import "sort"

type ConfigStateMachine interface {
	Query(num int) (Config, Err)
	Move(shard int, gid int) Err
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
}

type MemoryConfig struct {
	Configs []Config
}

func NewMemoryConfigs() *MemoryConfig {
	mcf := &MemoryConfig{make([]Config, 1)}
	mcf.Configs[0].Groups = map[int][]string{}
	return mcf
}

func (mcf *MemoryConfig) Query(num int) (Config, Err) {
	// 如果该数字为 -1 或大于已知的最大配置数字，则 shardctrler 应回复最新配置。
	if num < 0 || num >= len(mcf.Configs) {
		return mcf.Configs[len(mcf.Configs)-1], OK
	}
	return mcf.Configs[num], OK
}

func (mcf *MemoryConfig) Move(shard int, gid int) Err {
	lastConfig := mcf.Configs[len(mcf.Configs)-1]
	newConfig := Config{len(mcf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	mcf.Configs = append(mcf.Configs, newConfig)
	return OK
}

func (mcf *MemoryConfig) Join(groups map[int][]string) Err {
	lastConfig := mcf.Configs[len(mcf.Configs)-1]
	newConfig := Config{len(mcf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// balance
	g2s := groupToShards(newConfig)

	for {
		s, t := getMaxNumShardByGid(g2s), getMinNumShardByGid(g2s)
		if s != 0 && len(g2s[s])-len(g2s[t]) <= 1 {
			break
		}
		g2s[t] = append(g2s[t], g2s[s][0])
		g2s[s] = g2s[s][1:]
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shardId := range shards {
			newShards[shardId] = gid
		}
	}
	newConfig.Shards = newShards
	mcf.Configs = append(mcf.Configs, newConfig)
	return OK
}

func (mcf *MemoryConfig) Leave(gids []int) Err {
	lastConfig := mcf.Configs[len(mcf.Configs)-1]
	newConfig := Config{len(mcf.Configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	g2s := groupToShards(newConfig)

	noUsedShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := g2s[gid]; ok {
			noUsedShards = append(noUsedShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) > 0 {
		for _, shardId := range noUsedShards {
			t := getMinNumShardByGid(g2s)
			g2s[t] = append(g2s[t], shardId)
		}

		for gid, shards := range g2s {
			for _, shardId := range shards {
				newShards[shardId] = gid
			}
		}
	}
	newConfig.Shards = newShards
	mcf.Configs = append(mcf.Configs, newConfig)
	return OK
}

func getMinNumShardByGid(g2s map[int][]int) int {
	// 不固定顺序的话，可能会导致两次的config不同
	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	min, index := NShards+1, -1
	for _, gid := range gids {
		if gid != 0 && len(g2s[gid]) < min {
			min = len(g2s[gid])
			index = gid
		}
	}
	return index
}

func getMaxNumShardByGid(g2s map[int][]int) int {
	// GID 0是初始配置，一开始所有分片分配给 GID 0
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	gids := make([]int, 0)
	for key := range g2s {
		gids = append(gids, key)
	}

	sort.Ints(gids)

	max, index := -1, -1
	for _, gid := range gids {
		if len(g2s[gid]) > max {
			max = len(g2s[gid])
			index = gid
		}
	}
	return index
}
func groupToShards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shardId)
	}
	return g2s
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
