package mapreduce

import (
	"fmt"
	"sync"
)

type workerRetryCounter struct {
	sync.Mutex
	counter map[string]int
}

func (r *workerRetryCounter) inc(server string) {
	r.Lock()
	defer r.Unlock()
	r.counter[server]++
}

func (r *workerRetryCounter) get(server string) int {
	r.Lock()
	defer r.Unlock()
	return r.counter[server]
}

const MaxWorkersFailures = 5

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var waitGroup sync.WaitGroup
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// create ready channel
	readyChan := make(chan string, ntasks)
	retryChan := make(chan *DoTaskArgs, ntasks)
	failureCounts := workerRetryCounter{counter: make(map[string]int)}

	// create task list
	//tasks := make([]*DoTaskArgs, ntasks)
	var tasks []*DoTaskArgs
	for curr := 0; curr < ntasks; curr++ {
		args := &DoTaskArgs{
			JobName:       jobName,
			File:          mapFiles[curr],
			Phase:         phase,
			TaskNumber:    curr,
			NumOtherPhase: n_other,
		}
		//fmt.Printf("###### %+v %d\n", args, len(tasks))
		tasks = append(tasks, args)
	}

	// functor
	startTask := func(worker string, args *DoTaskArgs) {
		defer waitGroup.Done()
		success := call(worker, "Worker.DoTask", args, nil)
		readyChan <- worker
		if !success {
			fmt.Printf("Schedule: %v task #%d failed to execute by %s\n", phase, args.TaskNumber, worker)
			failureCounts.inc(worker)
			retryChan <- args
		}
	}

	for len(tasks) > 0 {
		select {
		case taskArgs := <-retryChan:
			tasks = append(tasks, taskArgs)

		case worker := <-registerChan:
			readyChan <- worker

		case worker := <-readyChan:
			if failureCounts.get(worker) < MaxWorkersFailures {
				waitGroup.Add(1)
				index := len(tasks) - 1
				args := tasks[index]
				//fmt.Printf("+++++++++ %+v %d\n", args, len(tasks))
				tasks = tasks[:index]
				go startTask(worker, args)
			} else {
				fmt.Printf("schedule %v, worker %s failed %d times and will no longer be used\n", phase, worker, MaxWorkersFailures)
			}
		}
	}

	waitGroup.Wait()

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)

}
