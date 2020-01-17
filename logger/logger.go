package logger

import (
	"os"

	"github.com/sirupsen/logrus/hooks/test"

	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func SetupLogger(configuredLevel string) {
	level, err := logrus.ParseLevel(configuredLevel)
	if err != nil {
		level = logrus.InfoLevel
	}

	logger = &logrus.Logger{
		Out:   os.Stdout,
		Hooks: make(logrus.LevelHooks),
		Level: level,
		Formatter: &logrus.TextFormatter{
			DisableTimestamp:       true,
			DisableLevelTruncation: true,
		},
	}
}

func SetDummyLogger() {
	logger, _ = test.NewNullLogger()
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Fatal(args ...interface{}) {
	logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}
