package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
  nMap := 0
  nReduce := 0
  var worker string

  for {
    select {
    case  worker = <-mr.registerChannel:
      fmt.Println("Register", worker)
    case  worker = <-mr.doJobChannel:
      fmt.Println("doJob", worker);
    }

    if nMap < mr.nMap {
      go func() {
        jobNumber := <-mr.jobNumberChannel
        args := &DoJobArgs{}
        args.File = mr.file
        args.Operation = Map
        args.JobNumber = jobNumber
        args.NumOtherPhase = mr.nReduce

        var reply DoJobReply
        ok := call(worker, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoJob %s error", worker)
        }
        mr.doJobChannel <- worker
      }()
      mr.jobNumberChannel <- nMap
      nMap += 1
    } else if nReduce < mr.nReduce {
      go func() {
        jobNumber := <-mr.jobNumberChannel
        args := &DoJobArgs{}
        args.File = mr.file
        args.Operation = Reduce
        args.JobNumber = jobNumber
        args.NumOtherPhase = mr.nMap

        var reply DoJobReply
        ok := call(worker, "Worker.DoJob", args, &reply)
        if ok == false {
          fmt.Printf("DoJob %s error", worker)
        }
        mr.doJobChannel <- worker
      }()
      mr.jobNumberChannel <- nReduce
      nReduce += 1
    } else {
      break
    }
  }
	return mr.KillWorkers()
}
