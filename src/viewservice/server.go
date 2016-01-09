package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	current_view       View
	server_info_map    map[string]time.Time
	curr_primary_acked bool
	is_inited          bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	if !vs.is_inited {
		vs.current_view.Viewnum = 1
		vs.current_view.Primary = args.Me
		vs.current_view.Backup = ""
		vs.is_inited = true
	} else {
		if vs.current_view.Primary == args.Me {
			if vs.current_view.Viewnum == args.Viewnum {
				vs.curr_primary_acked = true
			} else {
				if vs.curr_primary_acked {
					vs.current_view.Viewnum = vs.current_view.Viewnum + 1
					vs.current_view.Primary = vs.current_view.Backup
					vs.current_view.Backup = ""
					vs.curr_primary_acked = false
				} else {
					//	what the fuck
				}
			}
		} else if vs.current_view.Backup == args.Me {
			if vs.current_view.Viewnum == args.Viewnum {
				// do nothing
			} else {
				if vs.curr_primary_acked {
					vs.current_view.Viewnum = vs.current_view.Viewnum + 1
					vs.current_view.Backup = ""
					vs.curr_primary_acked = false
				} else {
					// what the fuck
				}
			}
		} else {
			// do nothing
		}
	}
	vs.server_info_map[args.Me] = time.Now()
	reply.View = vs.current_view
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.current_view
	vs.mu.Unlock()
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	if vs.curr_primary_acked && vs.is_inited {
		primary_time, pok := vs.server_info_map[vs.current_view.Primary]
		backup_time, bok := vs.server_info_map[vs.current_view.Backup]
		if pok && (primary_time.Add(DeadPings * PingInterval).Before(time.Now())) {
			vs.current_view.Viewnum = vs.current_view.Viewnum + 1
			vs.current_view.Primary = vs.current_view.Backup
			vs.current_view.Backup = ""
			vs.curr_primary_acked = false
		} else if pok && ((bok && backup_time.Add(DeadPings*PingInterval).Before(time.Now())) || !bok) {
			new_back := ""
			for k, v := range vs.server_info_map {
				if k != vs.current_view.Primary && k != vs.current_view.Backup && (v.Add(DeadPings * PingInterval).After(time.Now())) {
					new_back = k
				}
			}
			vs.current_view.Viewnum = vs.current_view.Viewnum + 1
			vs.current_view.Backup = new_back
			vs.curr_primary_acked = false
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.current_view.Viewnum = 0
	vs.current_view.Primary = ""
	vs.current_view.Backup = ""
	vs.server_info_map = make(map[string]time.Time)
	vs.curr_primary_acked = false
	vs.is_inited = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
