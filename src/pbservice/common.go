package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op           string
	IsClientReq  bool
	IsPrimaryReq bool
	RpcId        int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	IsClientReq  bool
	IsPrimaryReq bool
	PrimaryValue string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type NewBackupArgs struct {
	Data     map[string]string
	CheckRpc map[int64]bool
	RpcId    int64
}

type NewBackupReply struct {
	Err Err
}
