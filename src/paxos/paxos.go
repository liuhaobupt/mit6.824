package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	peersNum      int
	proposerInfo  map[int]ProposerInfo
	acceptorInfo  map[int]AcceptorInfo
	maxProposalId map[int]ProposalId
	maxSeq        int
}

type ProposalId struct {
	S    int64
	Addr string
}

type ProposerInfo struct {
	proposalValue interface{}
	prepareResp   map[string]PrepareResp
	decidedValue  interface{}
	acceptResp    map[string]bool
	decided       bool
	decidedResp   map[string]bool
}

type AcceptorInfo struct {
	acceptProposalId ProposalId
	acceptValue      interface{}
	decidedValue     interface{}
	decided          bool
}

type PrepareArgs struct {
	Seq        int
	ProposalId ProposalId
}

type PrepareResp struct {
	AcceptProposalId ProposalId
	AcceptValue      interface{}
	HasValue         bool
	Rejected         bool

	Decided	bool
	DecidedValue interface{}
}

type AcceptArgs struct {
	Seq          int
	ProposalId   ProposalId
	DecidedValue interface{}
}

type AcceptResp struct {
	Rejected bool
}

type DecidedArgs struct {
	Seq          int
	DecidedValue interface{}
}

type DecidedResp struct {
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.mu.Lock()
	if seq > px.maxSeq {
		px.maxSeq = seq
	}
	decided := px.isDecided(seq)
	var maxProposalId ProposalId
	if !decided {
		maxProposalId = px.maxProposalId[seq]
		maxProposalId.S = maxProposalId.S + 1
		maxProposalId.Addr = px.peers[px.me]
		px.maxProposalId[seq] = maxProposalId
		var proposerInfo ProposerInfo
		proposerInfo.proposalValue = v
		proposerInfo.prepareResp = make(map[string]PrepareResp)
		proposerInfo.acceptResp = make(map[string]bool)
		proposerInfo.decidedResp = make(map[string]bool)
		px.proposerInfo[seq] = proposerInfo
	}
	px.mu.Unlock()

	if !decided {
		go func() {
			for {
				px.mu.Lock()
				var rejected bool
				var ok bool
				if !px.isPrepareMajority(seq) {
					for _, peer := range px.peers {
						if _, ok = px.proposerInfo[seq].prepareResp[peer]; !ok {
							args := &PrepareArgs{}
							args.ProposalId = maxProposalId
							args.Seq = seq
							var reply PrepareResp
							px.mu.Unlock()
							ok = call(peer, "Paxos.Prepare", args, &reply)
							px.mu.Lock()
							if ok {
								rejected = reply.Rejected
								if rejected {
									proposerInfo := px.proposerInfo[seq]
									proposerInfo.decided = reply.Decided
									//px.proposerInfo[seq].decided = reply.Decided
									if reply.Decided {
										proposerInfo.decidedValue = reply.DecidedValue
									}
									px.proposerInfo[seq] = proposerInfo
									break
								} else {
									px.proposerInfo[seq].prepareResp[peer] = reply
								}
							}
						}
					}
				}

				if !rejected && px.isPrepareMajority(seq) && !px.isAcceptMajority(seq) {
					proposerInfo := px.proposerInfo[seq]
					proposerInfo.decidedValue = px.decidedValue(seq)
					px.proposerInfo[seq] = proposerInfo
					for _, peer := range px.peers {
						if _, ok = px.proposerInfo[seq].acceptResp[peer]; !ok {
							args := &AcceptArgs{}
							args.Seq = seq
							args.ProposalId = maxProposalId
							args.DecidedValue = px.proposerInfo[seq].decidedValue
							var reply AcceptResp
							px.mu.Unlock()
							ok = call(peer, "Paxos.Accept", args, &reply)
							px.mu.Lock()
							if ok {
								rejected = reply.Rejected
								if rejected {
									break
								} else {
									px.proposerInfo[seq].acceptResp[peer] = true
								}
							}
						}
					}
				}

				if !rejected && px.isAcceptMajority(seq) && !px.isDecidedAll(seq) {
					//fmt.Println("acceptResp", px.proposerInfo[seq])
					proposerInfo := px.proposerInfo[seq]
					proposerInfo.decided = true
					px.proposerInfo[seq] = proposerInfo
					for _, peer := range px.peers {
						if _, ok = px.proposerInfo[seq].decidedResp[peer]; !ok {
							args := &DecidedArgs{}
							args.Seq = seq
							args.DecidedValue = px.proposerInfo[seq].decidedValue
							var reply DecidedResp
							px.mu.Unlock()
							ok = call(peer, "Paxos.Decided", args, &reply)
							px.mu.Lock()
							if ok {
								px.proposerInfo[seq].decidedResp[peer] = true
							}
						}
					}
				}

				decidedAll := px.isDecidedAll(seq)
				px.mu.Unlock()
				if decidedAll || rejected {
					break
				}
			}
		}()
	}
}

func (px *Paxos) isDecided(seq int) bool {
	_, pOK := px.proposerInfo[seq]
	_, aOK := px.acceptorInfo[seq]
	decided := (pOK && px.proposerInfo[seq].decided) ||
		(aOK && px.acceptorInfo[seq].decided)
	return decided
}

func (px *Paxos) isDecidedAll(seq int) bool {
	return len(px.proposerInfo[seq].decidedResp) == px.peersNum
}

func (px *Paxos) isPrepareMajority(seq int) bool {
	return len(px.proposerInfo[seq].prepareResp) > px.peersNum/2
}

func (px *Paxos) isAcceptMajority(seq int) bool {
	return len(px.proposerInfo[seq].acceptResp) > px.peersNum/2
}

func (px *Paxos) decidedValue(seq int) interface{} {
	decidedValue := px.proposerInfo[seq].proposalValue
	var curProposalID ProposalId
	for _, prepareResp := range px.proposerInfo[seq].prepareResp {
		if prepareResp.HasValue && ProposalIDGreater(prepareResp.AcceptProposalId, curProposalID) {
			curProposalID = prepareResp.AcceptProposalId
			decidedValue = prepareResp.AcceptValue
		}
	}
	return decidedValue
}

func ProposalIDGreater(a ProposalId, b ProposalId) bool {
	if a.S > b.S {
		return true
	} else if a.S == b.S {
		return a.Addr > b.Addr
	} else {
		return false
	}
}

func ProposalIDGreaterEqual(a ProposalId, b ProposalId) bool {
	if a.S > b.S {
		return true
	} else if a.S == b.S {
		return a.Addr >= b.Addr
	} else {
		return false
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareResp) error {
	px.mu.Lock()
	seq := args.Seq
	proposalId := args.ProposalId
	if ProposalIDGreaterEqual(proposalId, px.maxProposalId[seq]) {
		px.maxProposalId[seq] = proposalId
		reply.Rejected = false
		_, ok := px.acceptorInfo[seq]
		if !ok {
			reply.HasValue = false
		} else {
			reply.HasValue = true
			reply.AcceptProposalId = px.acceptorInfo[seq].acceptProposalId
			reply.AcceptValue = px.acceptorInfo[seq].acceptValue
		}
	} else {
		reply.Rejected = true
		_, ok := px.acceptorInfo[seq]
		if ok {
			reply.Decided = px.acceptorInfo[seq].decided
			reply.DecidedValue = px.acceptorInfo[seq].decidedValue
		}
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptResp) error {
	px.mu.Lock()
	seq := args.Seq
	proposalId := args.ProposalId
	if ProposalIDGreaterEqual(proposalId, px.maxProposalId[seq]) {
		px.maxProposalId[seq] = proposalId
		reply.Rejected = false
		acceptorInfo := px.acceptorInfo[seq]
		acceptorInfo.acceptProposalId = proposalId
		acceptorInfo.acceptValue = args.DecidedValue
		px.acceptorInfo[seq] = acceptorInfo
	} else {
		reply.Rejected = true
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedResp) error {
	px.mu.Lock()
	seq := args.Seq
	acceptorInfo := px.acceptorInfo[seq]
	acceptorInfo.decided = true
	acceptorInfo.decidedValue = args.DecidedValue
	px.acceptorInfo[seq] = acceptorInfo
	px.mu.Unlock()
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	fate := Pending
	var retValue interface{}
	px.mu.Lock()
	if px.isDecided(seq) {
		fate = Decided
		retValue = px.getDecidedValue(seq)
	}
	px.mu.Unlock()
	return fate, retValue
}

func (px *Paxos) getDecidedValue(seq int) interface{} {
	_, pOK := px.proposerInfo[seq]
	if pOK && px.proposerInfo[seq].decided {
		//fmt.Println("proposerInfo", px.proposerInfo[seq].decidedValue)
		return px.proposerInfo[seq].decidedValue
	}
	_, aOK := px.acceptorInfo[seq]
	if aOK && px.acceptorInfo[seq].decided {
		//fmt.Println("acceptorInfo", px.acceptorInfo[seq].decidedValue)
		return px.acceptorInfo[seq].decidedValue
	}
	return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.peersNum = len(peers)
	px.proposerInfo = make(map[int]ProposerInfo)
	px.acceptorInfo = make(map[int]AcceptorInfo)
	px.maxProposalId = make(map[int]ProposalId)
	px.maxSeq = 0

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
