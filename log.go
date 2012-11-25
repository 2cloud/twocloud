package twocloud

import (
	"io/ioutil"
	"log"
	"os"
)

type Log struct {
	logger   *log.Logger
	logLevel logLevel
}

type logLevel int

const (
	LogLevelDebug = logLevel(iota)
	LogLevelWarn
	LogLevelError
)

func (l *Log) Debug(format string, v ...interface{}) {
	if l.logLevel <= LogLevelDebug {
		l.logger.Printf(format, v...)
	}
}

func (l *Log) Warn(format string, v ...interface{}) {
	if l.logLevel <= LogLevelWarn {
		l.logger.Printf(format, v...)
	}
}

func (l *Log) Error(format string, v ...interface{}) {
	if l.logLevel <= LogLevelError {
		l.logger.Printf(format, v...)
	}
}

func (l *Log) SetLogLevel(level logLevel) {
	l.logLevel = level
}

func StdOutLogger(level logLevel) *Log {
	return &Log{
		logger:   log.New(os.Stdout, "2cloud", log.LstdFlags|log.Llongfile),
		logLevel: level,
	}
}

func NullLogger() *Log {
	return &Log{
		logger:   log.New(ioutil.Discard, "2cloud", log.LstdFlags),
		logLevel: LogLevelError,
	}
}
