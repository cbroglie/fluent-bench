package batcher

import (
	"errors"
	"sync"
	"time"
)

// A Task is the interface representing a single unit of work. Callers add tasks
// one at a time, and they will be batched together and handed back to be
// processed by the handler function.
type Task interface {
}

// A Batch is an array of Task objects.
type Batch struct {
	Tasks []Task
}

// BatchHandler defines the signature for batch handler functions.
type BatchHandler func(context interface{}, b Batch)

// InitFunc is called once per worker on startup, and the worker passes its
// return value along each time it invokes the batch handler.
// It's useful if your workers need to manage their own resources.
type InitFunc func(id int) interface{}

// ShutdownFunc is called once per worker on shutdown with the return value from
// the init function.
// It's useful if your workers have to cleanup any resources.
type ShutdownFunc func(context interface{})

// B represents a single batcher instance. Instances must be created using the
// New function.
type B struct {
	numWorkers    int
	timeout       time.Duration
	maxBatchSize  int
	maxBatchDelay time.Duration
	inputChan     chan Task
	batchChan     chan Batch
	stopChan      chan bool
	doneChan      chan bool
	handler       BatchHandler
	initFn        InitFunc
	shutdownFn    ShutdownFunc
}

var (
	// ErrTimeout is returned when a timeout occurs trying to add a task to the
	// queue.
	ErrTimeout = errors.New("Timed out trying to add task to queue")
)

// New is used to create batcher instances. You can start adding tasks
// immediately, but they will not be processed until Start is invoked.
func New(numWorkers int, timeout time.Duration, maxBatchSize int, maxBatchDelay time.Duration, handler BatchHandler, initFn InitFunc, shutdownFn ShutdownFunc) *B {
	b := &B{
		numWorkers:    numWorkers,
		timeout:       timeout,
		maxBatchSize:  maxBatchSize,
		maxBatchDelay: maxBatchDelay,
		handler:       handler,
		initFn:        initFn,
		shutdownFn:    shutdownFn,
	}

	// User requests will come in over the input channel, where we will then
	// batch them together and send along to a worker. Buffer up to the batch
	// size, after that block.
	b.inputChan = make(chan Task, maxBatchSize)

	// The workers read from the batch channel as fast as they can, so we set
	// the buffer size so that there will be at most one batch available per
	// worker.
	b.batchChan = make(chan Batch, numWorkers)

	// The stop channel is used a signal to each worker to stop processing.
	b.stopChan = make(chan bool)

	// The done channel will be written to once all workers complete, which
	// they will do after seeing that the stop channel is closed.
	b.doneChan = make(chan bool)

	return b
}

// Start launches the worker goroutines. No tasks will be processed until Start
// is invoked. Calling it multiple times will result in multiple sets of workers
// being spawned.
func (b *B) Start() {
	var wg sync.WaitGroup
	wg.Add(b.numWorkers + 1)
	for i := 0; i < b.numWorkers; i++ {
		i := i
		go func() {
			b.worker(i)
			wg.Done()
		}()
	}

	go func() {
		b.batcher()
		wg.Done()
	}()

	go func() {
		wg.Wait()
		b.doneChan <- true
	}()
}

// Stop signals each worker goroutine to stop processing and waits for them to
// terminate.
func (b *B) Stop() {
	// This will signal the workers to shut down. Each will finish any inflight
	// requests, then stop.
	close(b.stopChan)

	// Block until all workers have finished.
	<-b.doneChan
}

// AddTask adds a new task to the queue for processing. It will block until the
// task is added to the queue, or the specified timeout is reached.
func (b *B) AddTask(t Task) error {
	select {
	case b.inputChan <- t:
		return nil
	case <-time.After(b.timeout):
		return ErrTimeout
	}
}

func (b *B) batcher() {
	// The objective is to batch as many Tasks together as we can, up to the max
	// batch size. But, once we receive the first task in a batch, we don't want
	// to delay more than maxBatchDelay before sending it, even if the batch
	// doesn't get filled.
	//
	// Attempted reads or sends from a nil channel are skipped over in a select
	// statement, so we toggle nil for the different channels to achieve the
	// desired batching behavior.
	var timerChan <-chan time.Time
	var inputChan <-chan Task
	var batchChan chan<- Batch

	// Start with the input channel enabled and an empty batch.
	inputChan = b.inputChan
	batch := Batch{}

	// Create a single timer instance to reuse rather than constantly recreating
	// it with calls to time.After. Call Stop immediately so no events are sent
	// to the channel until we call Reset to explicitly start it below.
	timer := time.NewTimer(b.maxBatchDelay)
	timer.Stop()

	for {
		// We loop forever reading from the input channel and assembling batches
		// to send along the batch channel, until the stop channel is closed.
		// It's important to keep the stop channel check first, otherwise we
		// could loop forever if data is continuously added to the input
		// channel.
		select {
		case _, ok := <-b.stopChan:
			if !ok {
				// Send any partial batch and then close the batch channel to
				// signal workers to complete.
				if len(batch.Tasks) > 0 {
					b.batchChan <- batch
				}
				close(b.batchChan)
				return
			}
		case task := <-inputChan:
			batch.Tasks = append(batch.Tasks, task)
			// If the batch is full, disable the input channel and enable the
			// batch channel so we can send the batch along. If instead this is
			// the first task in the batch, start the timer.
			if len(batch.Tasks) >= b.maxBatchSize {
				inputChan = nil
				batchChan = b.batchChan
				timerChan = nil
			} else if timerChan == nil {
				timer.Reset(b.maxBatchDelay)
				timerChan = timer.C
			}
		case <-timerChan:
			// Timer has expired, enable the batch channel to attempt to send
			// the batch the next time through the loop.
			if len(batch.Tasks) > 0 {
				batchChan = b.batchChan
			}
			timerChan = nil
		case batchChan <- batch:
			// We just sent a batch. Disable the batch channel and re-enable the
			// input channel so we can start receiving again. Stop the timer if
			// it's active.
			inputChan = b.inputChan
			batchChan = nil
			if timerChan != nil {
				timer.Stop()
				timerChan = nil
			}
			batch = Batch{}
		}
	}
}

func (b *B) worker(id int) {
	var context interface{}
	if b.initFn != nil {
		context = b.initFn(id)
	}

	// Read from the batch channel until it is closed, then exit.
	for batch := range b.batchChan {
		b.handler(context, batch)
	}

	if b.shutdownFn != nil {
		b.shutdownFn(context)
	}
}
