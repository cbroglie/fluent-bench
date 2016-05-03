package main

import (
	"flag"
	"log"
	"time"

	"github.com/cbroglie/fluent-bench/batcher"
	"github.com/rcrowley/go-metrics"
)

type stats struct {
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
	s.processed = metrics.NewTimer()
	s.succeeded = metrics.NewCounter()
	s.failed = metrics.NewCounter()
	metrics.Register("processed", s.processed)
	metrics.Register("succeeded", s.succeeded)
	metrics.Register("failed", s.failed)

	b = batcher.New(workers, timeout, batchSize, delay, worker(s, debug), initWorker, shutdownWorker)
	b.Start()

	batchSize, ticker := setThrottle(rate, nil)
	for i := 0; i < count; i++ {
		// Send the next record.
		b.AddTask(&message{
			Tag: "kinesis.test",
			Record: map[string]interface{}{
				"payload": "1,63,1,12213656201,Fuel,consumable,1,,0,Harvester,,,,null,100,2016-04-18,05:39:02,0,cash,1,128657335,,,0,,,0,,0,100,Counter,0,0,0,0,,null,null",
			},
		})

		// Rate limit before continuing.
		if ticker != nil && i%batchSize == 0 {
			<-ticker.C
		}
	}

	if debug {
		log.Println("completed work loop")
	}

	b.Stop()

	log.Printf("processed=%d p50=%s p99=%s\n",
		s.processed.Count(),
		formatDuration(s.processed.Percentile(0.50)),
		formatDuration(s.processed.Percentile(0.99)))
	log.Printf("succeeded=%d\n", s.succeeded.Count())
	log.Printf("failed=%d\n", s.failed.Count())
}

func formatDuration(duration float64) string {
	return time.Duration(duration).String()
}
