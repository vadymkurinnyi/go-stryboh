package telemetry

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"go.uber.org/fx"
)

type TelemetryDB struct {
	Connection driver.Conn
}

func NewTelemetryDB(lc fx.Lifecycle) (*TelemetryDB, error) {
	connection, err := duckdb.NewConnector("telemetry.db", nil)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connection)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS
		TelemetryData(
		DeviceID    VARCHAR,
		Temperature FLOAT,	
		Humidity    FLOAT,
		Pressuere   FLOAT,
		Mac         BIGINT,
		Timestamp   TIMESTAMP 
		)`); err != nil {
		return nil, err
	}
	con, err := connection.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return connection.Close()
		},
	})

	return &TelemetryDB{
		con,
	}, nil
}

func (db *TelemetryDB) SaveData(data []TelemeryData) error {
	appender, err := duckdb.NewAppenderFromConn(db.Connection, "", "TelemetryData")
	if err != nil {
		return err
	}
	defer appender.Close()
	for _, d := range data {
		if err := appender.AppendRow(
			d.DeviceID,
			d.Temperature,
			d.Humidity,
			d.Pressuere,
			d.Mac,
			time.Unix(d.Timestamp, 0)); err != nil {
			return err
		}
	}
	if err := appender.Flush(); err != nil {
		return err
	}
	return nil
}

func (db *TelemetryDB) Close() error {
	return db.Connection.Close()
}
