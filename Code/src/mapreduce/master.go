package mapreduce

import (
	"container/list"
)
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
	// Your code here

	// channel for workers that are ready
	readyWorkerChannel := make(chan string)

	// channel for queuing the DoWorkArgs
	argsChannel := make(chan *DoJobArgs)

	// channel for counting the finished workers
	// here it is reliable to use the count as the indicator of succeeded jobs: If a job delays and a duplicate is
	// launched and both jobs finished, since the first job returns false in call(), it will not be added to the
	// doneWorkerChannel, and only the successful job that ALSO return true in call() is added to the doneWorkerChannel
	doneWorkerChannel := make(chan int)

	// function to get the next worker
	// this function blocks until it receives a worker either from registerChannel or readyWorkerChannel
	getNextWorker := func() string {

		var workerAddress string

		select {
			// a newly registered worker comes in
			case workerAddress = <- mr.registerChannel:
				mr.Workers[workerAddress] = &WorkerInfo{workerAddress}
			// a worker finishes its job
			case workerAddress = <- readyWorkerChannel:
			// if not worker is available, the thread will block
		}
		return workerAddress
	}

	workerDoJob := func(workerAddress string, args *DoJobArgs) {
		doJobReply := DoJobReply{}
		ok := call(workerAddress, "Worker.DoJob", args, &doJobReply)
		// the call returns successfully
		if ok {
			/*
			If we put doneWorkerChannel before readyWorkerChannel, this will cause deadlock.
			If we put the workerAddress into the channel first, then the thread blocks until another worker is needed
			by the dispatcher thread. Then when the jobs are finished, all workers are trying to add to
			readyWorkerChannel but no worker is needed, hence all the workers will be blocked and cannot add to
			readyWorkerChannel.
			On the other hand, if we add to doneWorkerChannel first, since the main thread is always trying to retrieve
			from doneWorkerChannel, then adding to the doneWorkerChannel is never actually blocking.
			This is an example of necessity of paying attention to the order of adding to channel when a thread is
			trying to adding to multiple channels, since each channel could be blocking.
			 */
			doneWorkerChannel <- 1
			readyWorkerChannel <- workerAddress
		// the call fails
		} else {
			// put the args back to the channel
			fmt.Printf("ERROR: RunMaster RPC %s Worker.DoJob JobNumber %d\n", workerAddress, args.JobNumber)
			argsChannel <- args
			// TODO: Here we forever ignore the worker if we receive a false from RPC call. However, the worker might be only slow.
		}

	}

	/********************** starting to launch the threads ****************************/

	// start the job dispatcher
	// the job dispatcher does not care about whether it is map or reduce. It simply dispatches whatever args it
	// gets from the channel.
	go func() {
		for doJobArgs := range argsChannel {
			doJobArgs := doJobArgs
			// for each args, dispatch a thread
			go func() {
				// get a worker
				workerAddress := getNextWorker()
				// do the job
				workerDoJob(workerAddress, doJobArgs)
			}()
		}
	}()


	// start the thread of map job insertion
	go func() {
		for i := 0; i < mr.nMap; i++ {
			doJobArgs := DoJobArgs{File: mr.file, Operation: Map, JobNumber: i, NumOtherPhase: mr.nReduce}
			argsChannel <- &doJobArgs
		}
	}()


	// wait for map to finish
	for i := 0; i < mr.nMap; i++ {
		<- doneWorkerChannel
	}


	// start the thread of reduce job insertion
	// Since this follows the previous wait section, this ensures the reduce jobs only begins after all map jobs finish
	go func() {
		for i := 0; i < mr.nReduce; i++ {
			doJobArgs := DoJobArgs{File: mr.file, Operation: Reduce, JobNumber: i, NumOtherPhase: mr.nMap}
			argsChannel <- &doJobArgs
		}
	}()


	// wait for reduce to finish
	for i := 0; i < mr.nReduce; i++ {
		<- doneWorkerChannel
	}


	return mr.KillWorkers()
}
