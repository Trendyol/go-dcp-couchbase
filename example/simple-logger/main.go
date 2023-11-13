package main

import (
	"github.com/Trendyol/go-dcp-couchbase"
	"github.com/sirupsen/logrus"
)

func main() {
	logger := createLogger()
	connector, err := dcpcouchbase.NewConnectorBuilder("config.yml").
		SetMapper(dcpcouchbase.DefaultMapper).
		SetLogger(logger).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()

}

func createLogger() *logrus.Logger {
	logger := logrus.New()

	logger.SetLevel(logrus.ErrorLevel)
	formatter := &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyMsg:   "msg",
			logrus.FieldKeyLevel: "logLevel",
			logrus.FieldKeyTime:  "timestamp",
		},
	}

	logger.SetFormatter(formatter)
	return logger
}
