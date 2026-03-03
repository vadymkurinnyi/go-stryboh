package workers

import (
	"azurito/telemetry"
	"context"
	"encoding/json"
	"fmt"

	"github.com/amenzhinsky/iothub/eventhub"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type TelemetryParserWorker struct {
	hub    *eventhub.Client
	c      telemetry.TelemetryChanel
	logger *zap.Logger
}

func NewTelemetryParserWorker(hub *eventhub.Client, c telemetry.TelemetryChanel, logger *zap.Logger, lc fx.Lifecycle) *TelemetryParserWorker {
	worker := &TelemetryParserWorker{
		hub:    hub,
		c:      c,
		logger: logger,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				err := worker.hub.Subscribe(context.Background(), worker.parseEvent)
				if err != nil {
					worker.logger.Fatal(err.Error())
				}
			}()
			return nil
		},
	})
	return worker
}

func (w *TelemetryParserWorker) parseEvent(msg *eventhub.Event) error {
	var data telemetry.TelemeryData
	if e := json.Unmarshal(msg.GetData(), &data); e != nil {
		w.logger.Error(e.Error())
		return nil
	}
	w.c <- data
	w.logger.Info(fmt.Sprintf("mID: %v  data: %v", data.DeviceID, data.Temperature))
	return nil
}
