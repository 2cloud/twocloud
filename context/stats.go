// This package is heavily based on the NSQ statsd client: https://github.com/bitly/nsq/blob/master/util/statsd_client.go under the MIT license

package context

import (
	"fmt"
	"net"
	"time"
)

// Stats is a client that can increment, decrement, and send timing and gauge stats
// to a statsd server.
type Stats struct {
	conn   net.Conn
	addr   string
	prefix string
}

// NewStats returns a Stats client that will send data to the specified server.
func NewStats(addr, prefix string) Stats {
	return Stats{
		addr:   addr,
		prefix: prefix,
	}
}

// String returns a string representation of the Stats client.
func (s Stats) String() string {
	return s.addr
}

// CreateSocket opens the socket to send data to the statsd server.
func (s *Stats) CreateSocket() error {
	conn, err := net.DialTimeout("udp", s.addr, time.Second)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

// Close closes the socket that is sending data to the statsd server.
func (s Stats) Close() error {
	return s.Close()
}

// Increment increments the value of a single stat by the count specified.
func (s Stats) Increment(stat string, count int64) error {
	return s.send(stat, "%d|c", count)
}

// Decrement decrements the value of a single stat by the count specified.
// Count will be subtracted from the stat.
func (s Stats) Decrement(stat string, count int64) error {
	return s.send(stat, "%d|c", -count)
}

// Timing sends timing information to the statsd server for the stat specified.
func (s Stats) Timing(stat string, delta int64) error {
	return s.send(stat, "%d|ms", delta)
}

// Gauge sets the gauge value for the stat specified.
func (s Stats) Gauge(stat string, value int64) error {
	return s.send(stat, "%d|g", value)
}

func (s Stats) send(stat string, format string, value int64) error {
	if s.conn == nil {
		return nil
	}
	format = fmt.Sprintf("%s%s:%s", s.prefix, stat, format)
	_, err := fmt.Fprintf(s.conn, format, value)
	return err
}
