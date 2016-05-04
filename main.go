package main

import (
	"flag"
	"log"
	"sync"
	"time"

	"github.com/cbroglie/fluent-bench/batcher"
	"github.com/rcrowley/go-metrics"
)

type stats struct {
	published metrics.Counter
	processed metrics.Timer
	succeeded metrics.Counter
	failed    metrics.Counter
}

const (
	timeout          = 1 * time.Hour
	delay            = 1 * time.Hour
	batchSize        = 1
	maxEffectiveRate = 100 // anything higher than this the ticker is too coarse
)

var (
	rate    = 0
	workers = 10
	count   = 0
	debug   = false

	s = &stats{}
	b *batcher.B
)

func setThrottle(rate int, ticker *time.Ticker) (int, *time.Ticker) {
	if ticker != nil {
		ticker.Stop()
	}

	if rate == 0 {
		if debug {
			log.Printf("Throttling is off rate=%d", rate)
		}
		return 0, nil
	}

	if debug {
		log.Printf("Setting rate=%d", rate)
	}
	if rate <= maxEffectiveRate {
		batchSize := 1
		return batchSize, time.NewTicker(1e9 / time.Duration(rate))
	}
	// Timer resolution is about 10 ms. Rate higher than 100 rps will
	// require batching. It also introduces burstiness.
	batchSize := rate / maxEffectiveRate
	return batchSize, time.NewTicker(1e9 / time.Duration(maxEffectiveRate))
}

type workerContext struct {
	id int
	f  *fluent
}

func initWorker(id int) interface{} {
	if debug {
		log.Printf("[worker %d] Init\n", id)
	}

	f := &fluent{}
	if err := f.connect(); err != nil {
		log.Fatal(err)
	}

	return &workerContext{
		id: id,
		f:  f,
	}
}

func shutdownWorker(context interface{}) {
	wc := context.(*workerContext)

	if debug {
		log.Printf("[worker %d] Shutdown\n", wc.id)
	}

	wc.f.close()
}

func worker(stats *stats, debug bool) func(context interface{}, b batcher.Batch) {
	return func(context interface{}, b batcher.Batch) {
		wc := context.(*workerContext)
		for _, task := range b.Tasks {
			ts := time.Now()
			if debug {
				log.Printf("[worker %d] %s\n", wc.id, task)
			}
			m := task.(*message)
			m.Time = ts.Unix()
			err := wc.f.sendMessage(m)
			if err != nil {
				stats.failed.Inc(1)
			} else {
				stats.succeeded.Inc(1)
			}
			stats.processed.UpdateSince(ts)
		}
	}
}

func main() {
	// Parse command line arguments.
	flag.IntVar(&rate, "rate", rate, "max requests per second (0 is unlimited)")
	flag.IntVar(&workers, "workers", workers, "number of worker processes")
	flag.IntVar(&count, "count", count, "number of events to send to fluentd")
	flag.BoolVar(&debug, "debug", debug, "debug mode")
	flag.Parse()

	if count <= 0 {
		log.Fatal("count must be a positive integer")
	}

	// Push items onto the input channel as fast as we can.
	if debug {
		log.Printf("starting work loop rate=%d worker=%d", rate, workers)
	}

	// Hook in go-metrics
	s.published = metrics.NewCounter()
	s.processed = metrics.NewTimer()
	s.succeeded = metrics.NewCounter()
	s.failed = metrics.NewCounter()

	b = batcher.New(workers, timeout, batchSize, delay, worker(s, debug), initWorker, shutdownWorker)
	b.Start()

	var metricsWG sync.WaitGroup
	metricsWG.Add(1)
	metricsChan := make(chan struct{})
	go func() {
		defer metricsWG.Done()
		for {
			select {
			case _, ok := <-metricsChan:
				if !ok {
					return
				}
			case <-time.After(1 * time.Second):
				log.Printf("published=%-10d succeeded=%-10d failed=%-10d inflight=%-10d\n",
					s.published.Count(),
					s.succeeded.Count(),
					s.failed.Count(),
					s.published.Count()-s.processed.Count())
			}
		}
	}()

	batchSize, ticker := setThrottle(rate, nil)
	for i := 0; i < count; i++ {
		// Send the next record.
		b.AddTask(&message{
			Tag: "kinesis.test",
			Record: map[string]interface{}{
				"payload": "1,63,1,12213656201,Fuel,consumable,1,,0,Harvester,,,,null,100,2016-04-18,05:39:02,0,cash,1,128657335,,,0,,,0,,0,100,Counter,0,0,0,0,,null,null",
			},
		})
		s.published.Inc(1)

		// Rate limit before continuing.
		if ticker != nil && i%batchSize == 0 {
			<-ticker.C
		}
	}

	if debug {
		log.Println("completed work loop")
	}

	// Short delay to make sure the batcher has picked up everything from the
	// input channel (it only waits for in flight work to complete, it doesn't
	// drain the input channel).
	time.Sleep(10 * time.Millisecond)

	b.Stop()

	// Stop the metrics worker.
	close(metricsChan)
	metricsWG.Wait()

	log.Printf("published=%-10d succeeded=%-10d failed=%-10d inflight=%-10d\n",
		s.published.Count(),
		s.succeeded.Count(),
		s.failed.Count(),
		s.published.Count()-s.processed.Count())

	log.Printf("processed=%-10d p50=%-16s p99=%-16s\n",
		s.processed.Count(),
		formatDuration(s.processed.Percentile(0.50)),
		formatDuration(s.processed.Percentile(0.99)))
}

func formatDuration(duration float64) string {
	return time.Duration(duration).String()
}
