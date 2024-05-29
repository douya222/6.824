package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const InvalidGroup = 0

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	requestMap map[int64]*ShardCleck
	configs    []Config // indexed by config num
}

type ShardCleck struct {
	seqId       int
	msgUniqueId int
	messageCh   chan Op
}

const (
	T_Join  = "Join"
	T_Leave = "Leave"
	T_Move  = "Move"
	T_Query = "Query"
)

type T_Op string

type Op struct {
	// Your data here.
	Command T_Op
	SeqId   int
	CkId    int64
	Servers map[int][]string // T_Join
	GIDs    []int            // T_Remove
	Shard   int              // T_Move
	GID     int              // T_Move
}

func (sc *ShardCtrler) GetCk(ckId int64) *ShardCleck {
	ck, found := sc.requestMap[ckId]
	if !found {
		ck = new(ShardCleck)
		ck.messageCh = make(chan Op)
		sc.requestMap[ckId] = ck
	}
	return sc.requestMap[ckId]
}

func (sc *ShardCtrler) WaitApplyMsgByCh(ck *ShardCleck) (Op, bool) {
	startTerm, _ := sc.rf.GetState()
	timer := time.NewTimer(120 * time.Millisecond)
	for {
		select {
		case Msg := <-ck.messageCh:
			return Msg, false
		case <-timer.C:
			curTerm, isLeader := sc.rf.GetState()
			if curTerm != startTerm || !isLeader {
				sc.mu.Lock()
				ck.msgUniqueId = 0
				sc.mu.Unlock()
				return Op{}, true // 继续等待
			}
			timer.Reset(120 * time.Millisecond)
		}
	}

}

func (sc *ShardCtrler) NotifyApplyMsgByCh(ch chan Op, Msg Op) {
	timer := time.NewTimer(120 * time.Millisecond)
	select {
	case ch <- Msg:
		return
	case <-timer.C:
		// if notify timeout, then we ignore
		// because client probably send request to anthor server
		return
	}
}

func (sc *ShardCtrler) getGIDs() []int {
	conf := sc.getConfig(-1)
	gids := make([]int, 0)
	for gid, _ := range conf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids

}

func (sc *ShardCtrler) getConfig(confNumber int) Config {
	if confNumber == -1 || confNumber >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[confNumber]
}

func (sc *ShardCtrler) getGroupShards(gid int) []int {
	conf := sc.getConfig(-1)
	shards := make([]int, 0)
	for shard, shardGid := range conf.Shards {
		if gid == shardGid {
			shards = append(shards, shard)
		}
	}
	sort.Ints(shards)
	return shards
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		Servers: args.Servers,
		SeqId:   args.SeqId,
		Command: T_Join,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	// waiting,阻塞等待
	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	// 等待结束

	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		GIDs:    args.GIDs,
		SeqId:   args.SeqId,
		Command: T_Leave,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	// waiting,阻塞等待
	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	// 等待结束

	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		Shard:   args.Shard,
		GID:     args.GID,
		SeqId:   args.SeqId,
		Command: T_Move,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	// waiting,阻塞等待
	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	// 等待结束

	reply.WrongLeader = WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	logIndex, _, isLeader := sc.rf.Start(Op{
		SeqId:   args.SeqId,
		Command: T_Query,
		CkId:    args.CkId,
	})
	if !isLeader {
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}

	ck := sc.GetCk(args.CkId)
	ck.msgUniqueId = logIndex
	sc.mu.Unlock()

	// waiting,阻塞等待
	_, WrongLeader := sc.WaitApplyMsgByCh(ck)
	// 等待结束

	sc.mu.Lock()
	defer sc.mu.Unlock()
	reply.WrongLeader = WrongLeader
	reply.Config = sc.getConfig(args.Num)

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
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
	sc.requestMap = make(map[int64]*ShardCleck)
	go sc.processMsg()
	return sc
}

func (sc *ShardCtrler) processMsg() {
	for {
		applyMsg := <-sc.applyCh
		opMsg := applyMsg.Command.(Op)
		_, isLeader := sc.rf.GetState()
		sc.mu.Lock()
		ck := sc.GetCk(opMsg.CkId)
		if applyMsg.CommandIndex == ck.msgUniqueId && isLeader {
			sc.NotifyApplyMsgByCh(ck.messageCh, opMsg)
			ck.msgUniqueId = 0
		}
		// aleardy process
		if opMsg.SeqId < ck.seqId {
			sc.mu.Unlock()
			continue
		}

		ck.seqId = opMsg.SeqId + 1
		sc.invokeMsg(opMsg)
		sc.mu.Unlock()

	}
}

func (sc *ShardCtrler) invokeMsg(Msg Op) {
	switch Msg.Command {
	case T_Join:
		latestConf := sc.getConfig(-1)
		newGroups := make(map[int][]string)
		// merge new group
		for gid, servers := range Msg.Servers {
			newGroups[gid] = servers
		}
		// merge old group
		for gid, servers := range latestConf.Groups {
			newGroups[gid] = servers
		}
		// append new config
		config := Config{
			Num:    len(sc.configs),
			Groups: newGroups,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		// need rebalance now
		sc.rebalance()
	case T_Leave:
		latestConf := sc.getConfig(-1)
		newGroups := make(map[int][]string)
		for gid, servers := range latestConf.Groups {
			// not in the remove gids, then append to new config
			if !xIsInGroup(gid, Msg.GIDs) {
				newGroups[gid] = servers
			}
		}
		// append new config
		config := Config{
			Num:    len(sc.configs),
			Groups: newGroups,
			Shards: latestConf.Shards,
		}
		sc.configs = append(sc.configs, config)
		sc.rebalance()
	case T_Move:
		latestConf := sc.getConfig(-1)
		config := Config{
			Num:    len(sc.configs),
			Groups: latestConf.Groups,
			Shards: latestConf.Shards,
		}
		config.Shards[Msg.Shard] = Msg.GID // no need rebalance
		sc.configs = append(sc.configs, config)
	case T_Query:
		// nothing to do
	}
}

func (sc *ShardCtrler) rebalance() {
	// rebalance shard to groups
	latestConf := sc.getConfig(-1)
	// if all groups leave, reset all shards
	if len(latestConf.Groups) == 0 {
		for index, _ := range latestConf.Shards {
			latestConf.Shards[index] = InvalidGroup
		}
		return
	}
	// step 1 : collect invalid shard
	gids := sc.getGIDs()
	idleshards := make([]int, 0)
	// 1st loop collect not distribute shard
	for index, belongGroup := range latestConf.Shards {
		if belongGroup == InvalidGroup || !xIsInGroup(belongGroup, gids) {
			idleshards = append(idleshards, index)
		}
	}
	// 2nd loop collect rich groups
	avgShard := (len(latestConf.Shards) / len(gids))
	richshards, poorGroups := sc.collectRichShardsAndPoorGroups(gids, avgShard)
	idleshards = append(idleshards, richshards...)
	sort.Ints(idleshards)

	// To prevent differnt server have diff result, sort it
	poorGIDs := make([]int, 0)
	for gid := range poorGroups {
		poorGIDs = append(poorGIDs, gid)
	}
	sort.Ints(poorGIDs)

	allocIndex, i := 0, 0
	for _, gid := range poorGIDs {
		groupShardNum := poorGroups[gid]
		for i = allocIndex; i < len(idleshards); i++ {
			groupShardNum++
			latestConf.Shards[idleshards[i]] = gid
			if groupShardNum > avgShard {
				break
			}
		}
		allocIndex = i
	}

	// 3rd alloc left shard
	for ; allocIndex < len(idleshards); allocIndex++ {
		i = allocIndex % len(gids)
		latestConf.Shards[idleshards[allocIndex]] = gids[i]
	}

	sc.configs[len(sc.configs)-1] = latestConf

}

func (sc *ShardCtrler) collectRichShardsAndPoorGroups(gids []int, avgShard int) ([]int, map[int]int) {
	richShards := make([]int, 0)
	poorGroups := make(map[int]int)
	for _, gid := range gids {
		groupShards := sc.getGroupShards(gid)
		// DPrintf("[ShardCtrler-%d] rebalance groupShards=%v, avgShard=%d, gids=%v", sc.me, groupShards, avgShard, gids)
		overShards := len(groupShards) - avgShard
		for i := 0; i < overShards; i++ {
			richShards = append(richShards, groupShards[i])
		}
		if overShards < 0 {
			poorGroups[gid] = len(groupShards)
		}
	}
	return richShards, poorGroups
}

func xIsInGroup(x int, groups []int) bool {
	for _, gid := range groups {
		if x == gid {
			return true
		}
	}
	return false
}
