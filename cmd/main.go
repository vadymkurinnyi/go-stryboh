package main

import (
	"azurito/telemetry"
	"azurito/telemetry/workers"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/amenzhinsky/iothub/eventhub"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

func main() {
	fx.New(
		fx.Provide(
			telemetry.NewTelemetryDB,
			NewEventHubClient,
			zap.NewExample,
			//	NewWorker

			NewTelemetryChanel,
			workers.NewAggregatorWorker,
			workers.NewTelemetryParserWorker),
		// fx.Invoke(func(*Worker) {}),
		fx.Invoke(func(*workers.AggregatorWorker, *workers.TelemetryParserWorker) {}),
	).Run()
}

func NewEventHubClient() (*eventhub.Client, error) {
	cs := os.Getenv("IOTHUB_SERVICE_CONNECTION_STRING")
	return eventhub.DialConnectionString(cs)
}

func NewTelemetryChanel() telemetry.TelemetryChanel {
	c := make(telemetry.TelemetryChanel, 100)
	return c
}

type Worker struct {
	logger    *zap.Logger
	hub       *eventhub.Client
	cancel    context.CancelFunc
	telemetry chan telemetry.TelemeryData
}

func NewWorker(lc fx.Lifecycle, logger *zap.Logger, hub *eventhub.Client) *Worker {
	w := &Worker{
		logger:    logger,
		hub:       hub,
		telemetry: make(chan telemetry.TelemeryData, 100),
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			runCtx, cancel := context.WithCancel(context.Background())
			w.cancel = cancel
			go w.run(runCtx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			<-ctx.Done()
			logger.Info("stopped on Stop")

			return nil
		},
	})

	return w
}

func (w *Worker) run(ctx context.Context) {
	go w.subscribe(ctx)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Stopped by ctx OnStart")
			return
		case <-w.telemetry:
			continue
		default:
			// do work
			time.Sleep(1 * time.Second)
			w.logger.Info("Working..")
		}
	}
}

func (w *Worker) subscribe(ctx context.Context) {
	err := w.hub.Subscribe(ctx, w.parseEvent)
	if err != nil {
		w.logger.Fatal(err.Error())
	}
}

func (w *Worker) parseEvent(msg *eventhub.Event) error {
	var data telemetry.TelemeryData
	if e := json.Unmarshal(msg.GetData(), &data); e != nil {
		w.logger.Error(e.Error())
		return nil
	}
	w.telemetry <- data
	w.logger.Info(fmt.Sprintf("mID: %v  data: %v", data.DeviceID, data.Temperature))
	return nil
}
