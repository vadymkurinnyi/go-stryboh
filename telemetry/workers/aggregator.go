package workers

import (
	"azurito/telemetry"
	"context"
	"log"
	"time"

	"go.uber.org/fx"
)

type AggregatorWorker struct {
	c      telemetry.TelemetryChanel
	buff   []telemetry.TelemeryData
	ticker *time.Ticker
	db     *telemetry.TelemetryDB
	cancel context.CancelFunc
}

func NewAggregatorWorker(c telemetry.TelemetryChanel, db *telemetry.TelemetryDB, lc fx.Lifecycle) *AggregatorWorker {
	worker := &AggregatorWorker{
		c:      c,
		db:     db,
		buff:   make([]telemetry.TelemeryData, 0, 1000),
		ticker: time.NewTicker(20 * time.Second),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			runCtx, cancel := context.WithCancel(context.Background())
			worker.cancel = cancel
			go func() {
				for {
					select {
					case t := <-worker.c:
						worker.buff = append(worker.buff, t)
						if len(worker.buff) >= 1000 {
							worker.flushToDB()
							worker.buff = worker.buff[:0]
						}
					case <-worker.ticker.C:
						worker.flushToDB()
						worker.buff = worker.buff[:0]
					case <-runCtx.Done():
						println("stoped by runCtx")
						return
					}
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			worker.cancel()
			worker.ticker.Stop()
			return nil
		},
	})

	return worker
}

func (w *AggregatorWorker) flushToDB() {
	if len(w.buff) < 1 {
		return
	}
	if err := w.db.SaveData(w.buff); err != nil {
		log.Fatal(err)
	}
	log.Printf("%v telemetryData saved to DB", len(w.buff))
}
