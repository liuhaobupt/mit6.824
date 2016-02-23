package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	Me    int
	Id    int
	RpcId int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	applyPoint int
	identity   int
	data       map[string]string
	rpcIds     map[int64]bool
}

func (kv *KVPaxos) makeOp(key string, value string, rpc_id int64, op_arg string) Op {
	var op Op
	op.Key = key
	op.Value = value
	op.RpcId = rpc_id
	op.Op = op_arg
	op.Me = kv.me
	op.Id = kv.identity
	return op
}

func (kv *KVPaxos) getStatus(seq int) (interface{}, paxos.Fate) {
	to := 10 * time.Millisecond
	retry := 0
	for {
		retry++
		status, result := kv.px.Status(seq)
		if status == paxos.Decided || status == paxos.Forgotten {
			return result, status
		}
		time.Sleep(to)
		if retry == 10 {
			op := kv.makeOp("", "", 0, "Query")
			kv.px.Start(seq, op)
		}
	}
}

func (kv *KVPaxos) apply(op Op) {
	if op.Op != "Get" && op.Op != "Query" {
		_, ok := kv.rpcIds[op.RpcId]
		if !ok {
			tmp_value := ""
			if op.Op == "Append" {
				tmp_value = kv.data[op.Key]
			}
			tmp_value = tmp_value + op.Value
			kv.data[op.Key] = tmp_value
			kv.rpcIds[op.RpcId] = true
		}
	}
	kv.applyPoint++
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	for {
		kv.mu.Lock()
		op := kv.makeOp(args.Key, "", args.RpcId, "Get")
		kv.identity++
		kv.mu.Unlock()

		seq := kv.px.Max() + 1
		kv.px.Start(seq, op)

		result, status := kv.getStatus(seq)

		if status == paxos.Decided && result.(Op).Me == op.Me && result.(Op).Id == op.Id {
			kv.mu.Lock()
			for kv.applyPoint < seq {
				dst_apply := kv.applyPoint + 1
				dst_op, _ := kv.getStatus(dst_apply)
				kv.apply(dst_op.(Op))
			}
			value, ok := kv.data[op.Key]
			if ok {
				reply.Value = value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
			done_point := kv.applyPoint
			kv.mu.Unlock()
			kv.px.Done(done_point)
			break
		}
	}
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	for {
		kv.mu.Lock()
		op := kv.makeOp(args.Key, args.Value, args.RpcId, args.Op)
		kv.identity++
		kv.mu.Unlock()

		seq := kv.px.Max() + 1
		kv.px.Start(seq, op)

		result, status := kv.getStatus(seq)

		if status == paxos.Decided && result.(Op).Me == op.Me && result.(Op).Id == op.Id {
			kv.mu.Lock()
			if kv.applyPoint+1 == seq {
				kv.apply(op)
			}
			done_point := kv.applyPoint
			kv.mu.Unlock()
			kv.px.Done(done_point)
			reply.Err = OK
			break
		}
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.applyPoint = 0
	kv.identity = 0
	kv.data = make(map[string]string)
	kv.rpcIds = make(map[int64]bool)

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
