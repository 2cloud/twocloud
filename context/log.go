package context

import (
	"fmt"
	"io"
	"os"
)

type loglevel byte

const (
	ERROR loglevel = iota
	WARN
	DEBUG
)

// Log provides a way to conditionally write to an io.WriteCloser based on the configured level of logging.
type Log struct {
	Output io.WriteCloser
	Level  loglevel
}

// LogFromFile creates a Log that will write to the provided path with the provided level.
func LogFromFile(path string, level loglevel) (Log, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return Log{}, err
	}
	return Log{Output: file, Level: level}, nil
}

// StdOutLog returns a Log that will write to StdOut with the provided level.
func StdOutLog(level loglevel) Log {
	return Log{Output: os.Stdout, Level: level}
}

// CanWrite returns true if the Log io.WriteCloser can be written to.
func (l Log) CanWrite() bool {
	return l.Output == nil
}

// Close closes the Log's WriteCloser.
func (l Log) Close() error {
	return l.Output.Close()
}

// Error writes to the Log's io.WriteCloser if the Log's level is ERROR or higher.
func (l Log) Error(items ...interface{}) error {
	var err error
	if l.Level <= ERROR && l.CanWrite() {
		_, err = fmt.Fprint(l.Output, items...)
	}
	return err
}

// Errorf writes using the specified format string to the Log's io.WriteCloser if the Log's
// level is ERROR or higher.
func (l Log) Errorf(format string, items ...interface{}) error {
	var err error
	if l.Level <= ERROR && l.CanWrite() {
		_, err = fmt.Fprintf(l.Output, format, items...)
	}
	return err
}

// Warn writes to the Log's io.WriteCloser if the Log's level is WARN or higher.
func (l Log) Warn(items ...interface{}) error {
	var err error
	if l.Level <= WARN && l.CanWrite() {
		_, err = fmt.Fprint(l.Output, items...)
	}
	return err
}

// Warn writes using the specified format string to the Log's io.WriteCloser if the Log's
// level is WARN or higher.
func (l Log) Warnf(format string, items ...interface{}) error {
	var err error
	if l.Level <= WARN && l.CanWrite() {
		_, err = fmt.Fprintf(l.Output, format, items...)
	}
	return err
}

// Debug writes to the Log's io.WriteCloser if the Log's level is DEBUG or higher.
func (l Log) Debug(items ...interface{}) error {
	var err error
	if l.Level <= DEBUG && l.CanWrite() {
		_, err = fmt.Fprint(l.Output, items...)
	}
	return err
}

// Debugf writes using the specified format string to the Log's io.WriteCloser if the Log's
// level is DEBUG or higher.
func (l Log) Debugf(format string, items ...interface{}) error {
	var err error
	if l.Level <= DEBUG && l.CanWrite() {
		_, err = fmt.Fprintf(l.Output, format, items...)
	}
	return err
}
