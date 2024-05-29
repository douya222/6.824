package shardkv

import (
	"bytes"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Command string
	ClerkId int64
	SeqId   int
	LeaveOp ShardChangeOp
	JoinOp  ShardJoinOp
	InitOp  ShardInitOp
	ConfOp  ShardConfOp
}

type ShardChangeOp struct {
	LeaveShards    map[int][]int //
	WaitJoinShards []int
	Servers        map[int][]string
	ConfigNum      int
}

type ShardJoinOp struct {
	Shards     []int // move shard rpc
	ShardData  map[string]string
	ConfigNum  int
	RequestMap map[int64]int
}

type ShardInitOp struct {
	Shards    []int // init shards
	ConfigNum int
}

type ShardConfOp struct {
	ConfigNum int
}

type Notify struct {
	Msg    Op
	Result Err
}

type ShardKVClerk struct {
	seqId       int
	messageCh   chan Notify
	msgUniqueId int
}

type ShardContainer struct {
	RetainShards   []int
	TransferShards []int
	WaitJoinShards []int
	ConfigNum      int
	QueryDone      bool
	ShardReqSeqId  int   // for local start raft command
	ShardReqId     int64 // for local start a op
}

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
	dataSource    map[string]string       // db
	shardkvClerks map[int64]*ShardKVClerk // ckid to ck
	mck           *shardctrler.Clerk      // clerk
	shards        ShardContainer          // this group hold shards
	persister     *raft.Persister
}

func (kv *ShardKV) WaitApplyMsgByCh(ch chan Notify, ck *ShardKVClerk) Notify {
	startTerm, _ := kv.rf.GetState()
	timer := time.NewTimer(500 * time.Millisecond)
	for {
		select {
		case notify := <-ch:
			return notify
		case <-timer.C:
			curTerm, isLeader := kv.rf.GetState()
			if curTerm != startTerm || !isLeader {
				kv.mu.Lock()
				ck.msgUniqueId = 0
				kv.mu.Unlock()
				return Notify{Result: ErrWrongLeader}
			}
			timer.Reset(500 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) NotifyApplyMsgByCh(ch chan Notify, Msg Op) {
	// we wait 200ms
	// if notify timeout, then we ignore, because client probably send request to anthor server
	result := OK
	// check shard is already move ?
	if Msg.Command == "Get" || Msg.Command == "Put" || Msg.Command == "Append" {
		keyShard := key2shard(Msg.Key)
		if !isShardInGroup(keyShard, kv.shards.RetainShards) || isShardInGroup(keyShard, kv.shards.TransferShards) {
			result = ErrWrongGroup
		}
	}
	notify := Notify{
		Result: Err(result),
		Msg:    Msg,
	}
	timer := time.NewTimer(200 * time.Millisecond)
	select {
	case ch <- notify:
		// DPrintf("[ShardKV-%d-%d] NotifyApplyMsgByCh kv.shards=%v, key2shard(Msg.Key)=%d Notify=%v", kv.gid, kv.me, kv.shards, key2shard(Msg.Key), notify)
		return
	case <-timer.C:
		// DPrintf("[ShardKV-%d-%d] NotifyApplyMsgByCh Notify=%v, timeout", kv.gid, kv.me, notify)
		return
	}
}

func (kv *ShardKV) GetCk(ckId int64) *ShardKVClerk {
	ck, found := kv.shardkvClerks[ckId]
	if !found {
		ck = new(ShardKVClerk)
		ck.seqId = 0
		ck.messageCh = make(chan Notify)
		kv.shardkvClerks[ckId] = ck

	}
	return kv.shardkvClerks[ckId]
}

func isShardInGroup(shard int, dstShardGroup []int) bool {
	for _, dstShard := range dstShardGroup {
		if dstShard == shard {
			return true
		}
	}
	return false
}

/*
@note : check the request key is correct  ?
@retval : true mean message valid
*/
func (kv *ShardKV) isRequestKeyCorrect(key string) bool {
	// check key belong shard still hold in this group ?
	keyShard := key2shard(key)
	// check shard is transfering
	isShardTransfering := isShardInGroup(keyShard, kv.shards.TransferShards)
	return isShardInGroup(keyShard, kv.shards.RetainShards) && !isShardTransfering
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// start a command
	ck := kv.GetCk(args.ClerkId)
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Command: "Get",
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		ck.msgUniqueId = 0
		kv.mu.Unlock()
		return
	}
	ck.msgUniqueId = logIndex
	kv.mu.Unlock()

	// step 2 : parse op struct
	notify := kv.WaitApplyMsgByCh(ck.messageCh, ck)
	getMsg := notify.Msg

	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = notify.Result
	if reply.Err != OK {
		return
	}
	_, foundData := kv.dataSource[getMsg.Key]
	if !foundData {
		reply.Err = ErrNoKey
	} else {
		reply.Value = kv.dataSource[getMsg.Key]
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.isRequestKeyCorrect(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	// start a command
	logIndex, _, isLeader := kv.rf.Start(Op{
		Key:     args.Key,
		Value:   args.Value,
		Command: args.Op,
		ClerkId: args.ClerkId,
		SeqId:   args.SeqId,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	ck := kv.GetCk(args.ClerkId)
	ck.msgUniqueId = logIndex
	// DPrintf("[ShardKV-%d-%d] Received Req PutAppend %v, waiting logIndex=%d", kv.gid, kv.me, args, logIndex)
	kv.mu.Unlock()
	// step 2 : wait the channel
	reply.Err = OK
	notify := kv.WaitApplyMsgByCh(ck.messageCh, ck)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("[ShardKV-%d-%d] Recived Msg [PutAppend] from ck.putAppendCh args=%v, SeqId=%d, Msg=%v", kv.gid, kv.me, args, args.SeqId, notify.Msg)
	reply.Err = notify.Result
	if reply.Err != OK {
		return
	}
}

func (kv *ShardKV) readKVState(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// DPrintf("[ShardKV-%d-%d] read size=%d", kv.gid, kv.me, len(data))
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	cks := make(map[int64]int)
	dataSource := make(map[string]string)
	shards := ShardContainer{
		TransferShards: make([]int, 0),
		RetainShards:   make([]int, 0),
		ConfigNum:      1,
	}
	//var commitIndex int
	if d.Decode(&cks) != nil ||
		d.Decode(&dataSource) != nil ||
		d.Decode(&shards) != nil {
		// DPrintf("[readKVState] decode failed ...")
	} else {
		for ckId, seqId := range cks {
			kv.mu.Lock()
			ck := kv.GetCk(ckId)
			ck.seqId = seqId
			kv.mu.Unlock()
		}
		kv.mu.Lock()
		kv.dataSource = dataSource
		kv.shards = shards
		// DPrintf("[ShardKV-%d-%d] readKVState kv.shards=%v messageMap=%v dataSource=%v", kv.gid, kv.me, shards, kv.shardkvClerks, kv.dataSource)
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) saveKVState(index int) {
	if kv.maxraftstate == -1 {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.shardkvClerks {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	e.Encode(kv.shards)
	kv.rf.Snapshot(index, w.Bytes())
	// DPrintf("[ShardKV-%d-%d] Size=%d, Shards=%v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.shards)
}

func (kv *ShardKV) persist() {
	if kv.maxraftstate == -1 {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	cks := make(map[int64]int)
	for ckId, ck := range kv.shardkvClerks {
		cks[ckId] = ck.seqId
	}
	e.Encode(cks)
	e.Encode(kv.dataSource)
	e.Encode(kv.shards)
	kv.persister.SaveSnapshot(w.Bytes())
}

// read kv state and raft snapshot
func (kv *ShardKV) readState() {
	kv.readKVState(kv.persister.ReadSnapshot())
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.dataSource = make(map[string]string)
	kv.shardkvClerks = make(map[int64]*ShardKVClerk)
	kv.shards = ShardContainer{
		RetainShards:   make([]int, 0),
		TransferShards: make([]int, 0),
		ShardReqSeqId:  1,
		ConfigNum:      1,
	}
	kv.persister = persister

	kv.readState()

	go kv.recvMsg()
	// todo
	// go kv.intervalQueryConfig()

	return kv
}

func (kv *ShardKV) recvMsg() {
	for {
		applyMsg := <-kv.applyCh
		kv.processMsg(applyMsg)
	}
}

func (kv *ShardKV) processMsg(applyMsg raft.ApplyMsg) {
	if applyMsg.SnapshotValid {
		kv.readKVState(applyMsg.Snapshot)
		return
	}
	Msg := applyMsg.Command.(Op)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	if kv.needSnapshot() {
		kv.saveKVState(applyMsg.CommandIndex - 1)
	}

	ck := kv.GetCk(Msg.ClerkId)
	needNotify := ck.msgUniqueId == applyMsg.CommandIndex
	if isLeader && needNotify {
		ck.msgUniqueId = 0
		kv.NotifyApplyMsgByCh(ck.messageCh, Msg)
	}

	if Msg.SeqId < ck.seqId {
		return
	}

	succ := true
	switch Msg.Command {
	case "Put":
		if kv.isRequestKeyCorrect(Msg.Key) {
			kv.dataSource[Msg.Key] = Msg.Value
		} else {
			succ = false
		}
	case "Append":
		if kv.isRequestKeyCorrect(Msg.Key) {
			kv.dataSource[Msg.Key] += Msg.Value
		} else {
			succ = false
		}
	case "ShardJoin":
	case "ShardConf":
	case "ShardChange":
	case "ShardLeave":
	case "ShardInit":
	}
	if succ {
		ck.seqId = Msg.SeqId + 1
	}
	kv.persist()
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.persister.RaftStateSize()/4 >= kv.maxraftstate && kv.maxraftstate != -1
}
