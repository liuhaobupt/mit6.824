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
	for k := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", k)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(k, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", k)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
  for {
    var register_worker string
    var done_job_info JobInfo

    select {
    case  register_worker = <-mr.registerChannel:
      fmt.Println("Register", register_worker)
      mr.Workers[register_worker] = 0
    case  done_job_info = <-mr.DoneJobChannel:
      fmt.Println("DoneJobChannel", done_job_info)
      if (done_job_info.Done) {
        mr.Workers[done_job_info.Worker] = 0
        if (done_job_info.Operation == Map) {
          mr.MapJobStat[done_job_info.JobNumber] = 2
        } else if (done_job_info.Operation == Reduce) {
          mr.ReduceJobStat[done_job_info.JobNumber] = 2
        }
      } else {
        delete(mr.Workers, done_job_info.Worker)
        if (done_job_info.Operation == Map) {
          mr.MapJobStat[done_job_info.JobNumber] = 0
        } else if (done_job_info.Operation == Reduce) {
          mr.ReduceJobStat[done_job_info.JobNumber] = 0
        }
      }
    }

    if !IsAllJobDone(&mr.MapJobStat) {
      var unbegin_job int
      var avail_worker string
      var has_job, has_worker bool
      unbegin_job, has_job = HasJobUnbegin(&mr.MapJobStat)
      avail_worker, has_worker = HasWorker(&mr.Workers)

      if has_job && has_worker {
        go func() {
          assign_job_info := <-mr.AssignJobChannel
          args := &DoJobArgs{}
          args.File = mr.file
          args.Operation = Map
          args.JobNumber = assign_job_info.JobNumber
          args.NumOtherPhase = mr.nReduce

          var reply DoJobReply
          done_job_info := assign_job_info
          ok := call(assign_job_info.Worker, "Worker.DoJob", args, &reply)
          if ok == false {
            done_job_info.Done = false
          } else {
            done_job_info.Done = true
          }
          mr.DoneJobChannel <- done_job_info
        }()
        mr.MapJobStat[unbegin_job] = 1
        mr.Workers[avail_worker] = 1
        mr.AssignJobChannel <- JobInfo {Map, unbegin_job, avail_worker, false}
      }
    } else if !IsAllJobDone(&mr.ReduceJobStat) {
      var unbegin_job int
      var avail_worker string
      var has_job, has_worker bool
      unbegin_job, has_job = HasJobUnbegin(&mr.ReduceJobStat)
      avail_worker, has_worker = HasWorker(&mr.Workers)

      if has_job && has_worker {
        go func() {
          assign_job_info := <-mr.AssignJobChannel
          args := &DoJobArgs{}
          args.File = mr.file
          args.Operation = Reduce
          args.JobNumber = assign_job_info.JobNumber
          args.NumOtherPhase = mr.nMap

          var reply DoJobReply
          done_job_info := assign_job_info
          ok := call(assign_job_info.Worker, "Worker.DoJob", args, &reply)
          if ok == false {
            done_job_info.Done = false
          } else {
            done_job_info.Done = true
          }
          mr.DoneJobChannel <- done_job_info
        }()
        mr.ReduceJobStat[unbegin_job] = 1
        mr.Workers[avail_worker] = 1
        mr.AssignJobChannel <- JobInfo {Reduce, unbegin_job, avail_worker, false}
      }
    } else {
      break
    }
  }
	return mr.KillWorkers()
}

func IsAllJobDone(job_stat *map[int]int) (bool) {
  ret := true
  for k := range *job_stat {
    if ((*job_stat)[k] != 2) {
      ret = false
      break
    }
  }
  return ret
}

func HasJobUnbegin(job_stat *map[int]int) (int, bool) {
  var job_number int
  has_job := false
  for k := range *job_stat {
    if ((*job_stat)[k] == 0) {
      job_number = k
      has_job = true
      break
    }
  }
  return job_number, has_job
}

func HasWorker(workers *map[string]int) (string, bool) {
  var worker string
  has_worker := false
  for k := range *workers {
    if ((*workers)[k] == 0) {
      worker = k
      has_worker = true
      break
    }
  }
  return worker, has_worker
}
