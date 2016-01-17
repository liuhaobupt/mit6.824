package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currViewnum uint
	currView    viewservice.View
	is_primary  bool
	is_backup   bool
	data        map[string]string
	checkRpc    map[int64]bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.is_primary && args.IsClientReq {
		value, ok := pb.data[args.Key]
		b_ok := true
		need_switch := false
		if pb.currView.Backup != "" {
			backup_args := &GetArgs{}
			backup_args.Key = args.Key
			backup_args.IsClientReq = false
			backup_args.IsPrimaryReq = true
			backup_args.PrimaryValue = value
			var backup_reply GetReply
			backup_ok := call(pb.currView.Backup, "PBServer.Get", backup_args, &backup_reply)
			if backup_ok == false || backup_reply.Err != OK || backup_reply.Value != value {
				b_ok = false
				if backup_ok && backup_reply.Err == OK && backup_reply.Value != value {
					need_switch = true
				}
			}
		}
		if !b_ok {
			reply.Err = ErrWrongServer
			reply.Value = ""
			if need_switch {
				pb.is_primary = false
				pb.currViewnum = 0
			}
		} else if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = value
		}
	} else if pb.is_backup && args.IsPrimaryReq {
		reply.Err = OK
		backup_value, ok := pb.data[args.Key]
		if ok {
			reply.Value = backup_value
		} else {
			reply.Value = ""
		}
	} else {
		reply.Err = ErrWrongServer
		reply.Value = ""
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	if pb.is_primary && args.IsClientReq {
		b_ok := true
		_, rpc_ok := pb.checkRpc[args.RpcId]
		if !rpc_ok {
			if pb.currView.Backup != "" {
				backup_args := &PutAppendArgs{}
				backup_args.Key = args.Key
				backup_args.Value = args.Value
				backup_args.Op = args.Op
				backup_args.IsClientReq = false
				backup_args.IsPrimaryReq = true
				backup_args.RpcId = args.RpcId
				var backup_reply PutAppendReply
				backup_ok := call(pb.currView.Backup, "PBServer.PutAppend", backup_args, &backup_reply)
				if backup_ok == false || backup_reply.Err != OK {
					b_ok = false
				}
			}
			if !b_ok {
				reply.Err = ErrWrongServer
			} else {
				tmp_value := ""
				if args.Op == "Append" {
					value, ok := pb.data[args.Key]
					if ok {
						tmp_value = value
					}
				}
				tmp_value = tmp_value + args.Value
				pb.data[args.Key] = tmp_value
				reply.Err = OK
				pb.checkRpc[args.RpcId] = true
			}
		} else {
			reply.Err = OK
		}
	} else if pb.is_backup && args.IsPrimaryReq {
		_, rpc_ok := pb.checkRpc[args.RpcId]
		if !rpc_ok {
			tmp_value := ""
			if args.Op == "Append" {
				value, ok := pb.data[args.Key]
				if ok {
					tmp_value = value
				}
			}
			tmp_value = tmp_value + args.Value
			pb.data[args.Key] = tmp_value
			pb.checkRpc[args.RpcId] = true
			reply.Err = OK
		} else {
			reply.Err = OK
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) callNewBackup() bool {
	args := NewBackupArgs{}
	args.Data = pb.data
	args.CheckRpc = pb.checkRpc
	args.RpcId = nrand()
	var reply NewBackupReply
	var i int
	for {
		i = i + 1
		ok := call(pb.currView.Backup, "PBServer.NewBackupHandler", args, &reply)
		if ok && reply.Err == OK {
			break
		}
		if i > 100 {
			break
		}
	}
	return true
}

func (pb *PBServer) NewBackupHandler(args *NewBackupArgs, reply *NewBackupReply) error {
	pb.mu.Lock()
	pb.data = args.Data
	pb.checkRpc = args.CheckRpc
	reply.Err = OK
	pb.mu.Unlock()
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	tmp_view, _ := pb.vs.Ping(pb.currViewnum)
	if tmp_view != pb.currView || tmp_view.Viewnum != pb.currViewnum {
		pb.currView = tmp_view
		pb.currViewnum = pb.currView.Viewnum
		if tmp_view.Primary == pb.me {
			pb.is_primary = true
			pb.is_backup = false
			if tmp_view.Backup != "" {
				pb.callNewBackup()
			}
		} else if tmp_view.Backup == pb.me {
			pb.is_primary = false
			pb.is_backup = true
		} else {
			pb.is_primary = false
			pb.is_backup = false
		}
	}
	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currViewnum = 0
	pb.is_primary = false
	pb.is_backup = false
	pb.data = make(map[string]string)
	pb.checkRpc = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
