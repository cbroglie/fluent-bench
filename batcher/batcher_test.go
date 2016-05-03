package batcher

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gopkg.in/check.v1"
)

type BatcherSuite struct {
}

var _ = check.Suite(&BatcherSuite{})

func Test(t *testing.T) {
	check.TestingT(t)
}

// TestBatcher validates the batching logic.
func (s *BatcherSuite) TestBatcher(c *check.C) {
	var batches []Batch
	numWorkers := 1
	timeout := time.Second
	maxBatchSize := 3
	maxBatchDelay := 5 * time.Millisecond
	handler := func(context interface{}, b Batch) {
		batches = append(batches, b)
	}
	batcher := New(numWorkers, timeout, maxBatchSize, maxBatchDelay, handler, nil, nil)
	batcher.Start()
	// Send 2 Tasks, then sleep past the max batch delay to ensure the partial
	// batch is sent.
	batcher.AddTask(1)
	batcher.AddTask(2)
	time.Sleep(maxBatchDelay * 2)
	// Send 4 Tasks, and ensure that a batch of 3 and then a batch of 1 are
	// sent.
	batcher.AddTask(3)
	batcher.AddTask(4)
	batcher.AddTask(5)
	batcher.AddTask(6)
	// Small sleep to make sure everything is read off of the input queue.
	time.Sleep(time.Millisecond)
	// Wait for workers to shutdown.
	batcher.Stop()

	c.Assert(len(batches), check.Equals, 3)
	c.Assert(len(batches[0].Tasks), check.Equals, 2)
	c.Assert(len(batches[1].Tasks), check.Equals, 3)
	c.Assert(len(batches[2].Tasks), check.Equals, 1)
	c.Assert(batches[0].Tasks[0].(int), check.Equals, 1)
	c.Assert(batches[0].Tasks[1].(int), check.Equals, 2)
	c.Assert(batches[1].Tasks[0].(int), check.Equals, 3)
	c.Assert(batches[1].Tasks[1].(int), check.Equals, 4)
	c.Assert(batches[1].Tasks[2].(int), check.Equals, 5)
	c.Assert(batches[2].Tasks[0].(int), check.Equals, 6)
}

// TestTimeout asserts that we correctly timeout requests that are queued for
// too long before being read from the input channel.
func (s *BatcherSuite) TestTimeout(c *check.C) {
	// Run 10 times to catch race conditions which don't always occur.
	for i := 0; i < 10; i++ {
		numWorkers := 1
		timeout := 5 * time.Millisecond
		maxBatchSize := 3
		maxBatchDelay := 5 * timeout
		handler := func(context interface{}, b Batch) {
			time.Sleep(5 * timeout)
			c.Assert(len(b.Tasks), check.Equals, maxBatchSize)
		}
		batcher := New(numWorkers, timeout, maxBatchSize, maxBatchDelay, handler, nil, nil)
		batcher.Start()

		numTimeouts := 5
		numRequests := numWorkers * maxBatchSize // what each worker can do
		numRequests += numWorkers * maxBatchSize // 1 batch buffered per worker
		numRequests += maxBatchSize              // batch the batcher can hold on to
		numRequests += maxBatchSize              // buffer for the input channel
		numRequests += numTimeouts

		var wg sync.WaitGroup
		wg.Add(numRequests)
		errs := make([]error, numRequests)
		for i := 0; i < numRequests; i++ {
			i := i
			go func() {
				errs[i] = batcher.AddTask(struct{}{})
				wg.Done()
			}()
		}

		wg.Wait()
		batcher.Stop()

		numErrors := 0
		for i := 0; i < numRequests; i++ {
			if errs[i] != nil {
				numErrors++
			}
		}

		c.Assert(numErrors, check.Equals, numTimeouts)
	}
}

// TestIssue1 asserts we don't send empty batches due to
// https://github-ca.corp.zynga.com/GameServices/go-batcher/issues/1
func (s *BatcherSuite) TestIssue1(c *check.C) {
	// Run 10 times to catch race conditions which don't always occur.
	for i := 0; i < 10; i++ {
		numWorkers := 1
		timeout := 10 * time.Millisecond
		maxBatchSize := 3
		maxBatchDelay := 2 * time.Millisecond

		var wg sync.WaitGroup
		wg.Add(1)
		var errs []error
		var m sync.Mutex
		handler := func(context interface{}, b Batch) {
			// First we block until told otherwise.
			wg.Wait()
			// Now validate the batch size.
			if len(b.Tasks) > maxBatchSize || len(b.Tasks) == 0 {
				m.Lock()
				defer m.Unlock()
				errs = append(errs, fmt.Errorf("Received batch size of %d, allowed ranged is [1,%d]", len(b.Tasks), maxBatchSize))
			}
		}

		batcher := New(numWorkers, timeout, maxBatchSize, maxBatchDelay, handler, nil, nil)
		batcher.Start()

		// Step 1: send enough requests to fill up the batcher channel,
		// inlcuding the buffer, and then 1 more.
		numRequests := numWorkers * maxBatchSize // what each worker can do
		numRequests += numWorkers * maxBatchSize // 1 batch buffered per worker
		numRequests += 1
		for i := 0; i < numRequests; i++ {
			err := batcher.AddTask(struct{}{})
			if err != nil {
				c.Fatal(err)
			}
		}

		// Step 2: sleep past the max batch delay, timer goes off.
		time.Sleep(2 * maxBatchDelay)

		// Step 3: new event comes in over inputChan, restarts timer.
		err := batcher.AddTask(struct{}{})
		if err != nil {
			c.Fatal(err)
		}

		// Step 4: batch channel becomes available (before timer expires), and
		// the batches are processed.
		wg.Done()

		// Step 5: timer goes off and we attempt to send an empty batch.
		time.Sleep(2 * maxBatchDelay)

		// Wait for everything to finish.
		batcher.Stop()

		// Check for errors.
		for _, err := range errs {
			c.Fatal(err)
		}
	}
}
