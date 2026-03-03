package telemetry

type TelemeryData struct {
	DeviceID    string  `json:"DeviceId"`
	Temperature float32 `json:"Temperature"`
	Humidity    float32 `json:"Humidity"`
	Pressuere   float32 `json:"Pressuere"`
	Mac         int64   `json:"Mac"`
	Timestamp   int64   `json:"TimeStamp"`
}

type TelemetryChanel chan TelemeryData
