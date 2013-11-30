package twocloud

import (
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/bradrydzewski/go.stripe"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/lib/pq"
	"github.com/noeq/noeq"
	"github.com/secondbit/go-nsq"
	"log"
	"log/syslog"
	"strings"
	"time"
)

type Persister struct {
	Generator *noeq.Client
	Database  *sql.DB
	Config    Config
	Log       Log
	Publisher *nsq.Writer
	Stats     *statsd.Client
}

type ID uint64

var IDBufferOverflow = errors.New("ID was more than 10 bytes long.")

const NSQTopic = "api_events"

const StatsDPrefix = "2cloud"

func (id *ID) IsZero() bool {
	return *id == ID(0)
}

func (id *ID) String() string {
	resp := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(resp, uint64(*id))
	return hex.EncodeToString(resp)
}

func IDFromString(input string) (ID, error) {
	bytes, err := hex.DecodeString(input)
	if err != nil {
		return ID(0), err
	}
	resp, numBytes := binary.Uvarint(bytes)
	if numBytes <= 0 {
		return ID(0), IDBufferOverflow
	}
	return ID(resp), nil
}

func NewPersister(config Config) (*Persister, error) {
	generatorConfig := config.Generator
	if generatorConfig.Address == "" {
		generatorConfig = IDGenerator{
			Address: "localhost:4444",
			Token:   "",
		}
	}
	generator, err := noeq.New(generatorConfig.Token, generatorConfig.Address)
	if err != nil {
		return nil, err
	}
	databaseConfig := config.Database
	if strings.HasPrefix(databaseConfig, "postgres://") {
		databaseConfig, err = pq.ParseURL(databaseConfig)
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("postgres", databaseConfig)
	if err != nil {
		return nil, err
	}
	var statsDClient *statsd.Client
	if config.Stats != "" {
		statsDClient, err = statsd.Dial(config.Stats, StatsDPrefix)
		if err != nil {
			return nil, err
		}
	}
	nsqConfig := config.NSQ
	var publisher *nsq.Writer
	if nsqConfig.Address != "" {
		publisher = nsq.NewWriter(nsqConfig.Address)
	}
	logConfig := config.Log
	var logger Log
	level := LogLevelError
	switch logConfig.Level {
	case "debug":
		level = LogLevelDebug
		break
	case "warn":
		level = LogLevelWarn
		break
	case "error":
		level = LogLevelError
		break
	}
	if logConfig.File == "" {
		logger = StdOutLogger(level)
	} else if logConfig.File == "syslog" {
		var priority syslog.Priority
		switch level {
		case LogLevelDebug:
			priority = syslog.LOG_DEBUG
		case LogLevelWarn:
			priority = syslog.LOG_WARNING
		case LogLevelError:
			priority = syslog.LOG_ERR
		}
		slog, err := syslog.NewLogger(priority, log.LstdFlags)
		if err != nil {
			return nil, err
		}
		logger = Log{
			logger:     slog,
			logLevel:   level,
			needsClose: false,
		}
	} else {
		logger, err = FileLogger(logConfig.File, level)
		if err != nil {
			return nil, err
		}
	}
	stripe.SetKey(config.Stripe)
	return &Persister{
		Generator: generator,
		Database:  db,
		Config:    config,
		Log:       logger,
		Publisher: publisher,
		Stats:     statsDClient,
	}, nil
}

var UnknownNSQError = errors.New("Unknown NSQ error.")
var UnknownNSQFrameError = errors.New("Unknown NSQ frame type returned.")

type NSQEvent struct {
	Topic  string `json:"topic,omitempty"`
	User   *ID    `json:"user,omitempty"`
	Device *ID    `json:"device,omitempty"`
	Record *ID    `json:"id,omitempty"`
}

func (persister *Persister) Publish(topic string, user, device, record *ID) ([]byte, error) {
	if persister.Publisher == nil {
		return []byte{}, nil
	}
	n := NSQEvent{
		Topic:  topic,
		User:   user,
		Device: device,
		Record: record,
	}
	body, err := json.Marshal(n)
	if err != nil {
		return []byte{}, err
	}
	respType, data, err := persister.Publisher.Publish(NSQTopic, body)
	switch respType {
	case nsq.FrameTypeResponse:
		return data, nil
	case nsq.FrameTypeError:
		if err == nil {
			err = UnknownNSQError
		}
		return []byte{}, err
	case nsq.FrameTypeMessage:
		return data, nil
	default:
		return []byte{}, UnknownNSQFrameError
	}
	return []byte{}, nil
}

func (persister *Persister) Close() {
	persister.Database.Close()
	if persister.Log.needsClose {
		persister.Log.Close()
	}
	if persister.Publisher != nil {
		persister.Publisher.Stop()
	}
	if persister.Stats != nil {
		persister.Stats.Close()
	}
}

func (persister Persister) GetID() (ID, error) {
	var id uint64
	var err error
	for trys := 5; trys > 0; trys-- {
		id, err = persister.Generator.GenOne()
		if err != nil {
			persister.Log.Error(err.Error())
			continue
		}
		return ID(id), err
	}
	persister.Log.Error("No ID generated.")
	return ID(id), err
}

func (persister Persister) Time(start time.Time, name string) {
	if persister.Stats == nil {
		return
	}
	elapsed := int64(time.Since(start) / time.Millisecond)
	err := persister.Stats.Timing(name, elapsed, 1.0)
	if err != nil {
		persister.Log.Error("Error logging timer: %s", err.Error())
	}
}

func (persister Persister) IncrementStat(name string, amount int64) {
	if persister.Stats == nil {
		return
	}
	err := persister.Stats.Inc(name, amount, 1.0)
	if err != nil {
		persister.Log.Error("Error incrementing stat: %s", err.Error())
	}
}

func (persister Persister) DecrementStat(name string, amount int64) {
	if persister.Stats == nil {
		return
	}
	err := persister.Stats.Dec(name, amount, 1.0)
	if err != nil {
		persister.Log.Error("Error decrementing stat: %s", err.Error())
	}
}

func (persister Persister) SetGaugeStat(name string, amount int64) {
	if persister.Stats == nil {
		return
	}
	err := persister.Stats.Gauge(name, amount, 1.0)
	if err != nil {
		persister.Log.Error("Error setting stat: %s", err.Error())
	}
}

type ScannableRow interface {
	Scan(dest ...interface{}) error
}

var pgErrCodeByteKey = byte('C')
var pgUniquenessErrCode = "23505"

// Used to test when an insert fails because of the UNIQUE constraint
func isUniqueConflictError(err error) bool {
	if err == nil {
		return false
	}
	pqErr, ok := err.(pq.PGError)
	if ok && pqErr.Get(pgErrCodeByteKey) == pgUniquenessErrCode {
		return true
	}
	return false
}
