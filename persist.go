package twocloud

import (
	"database/sql"
	"github.com/bmizerany/pq"
	"github.com/noeq/noeq"
	"log"
	"net/http"
	"os"
	"strings"
)

type Persister struct {
	Generator *noeq.Client
	Database  *sql.DB
	Config    Config
	Log       Log
	Auditor   *Auditor
	Request   *http.Request
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
	} else {
		fileWriter, err := os.Create(logConfig.File)
		if err != nil {
			return nil, err
		}
		defer fileWriter.Close()
		logger = Log{
			logger:   log.New(fileWriter, "2cloud", log.LstdFlags),
			logLevel: level,
		}
	}
	auditor, err := NewAuditor(config.Auditor)
	if err != nil {
		return nil, err
	}
	return &Persister{
		Generator: generator,
		Database:  db,
		Config:    config,
		Log:       logger,
		Auditor:   auditor,
		Request:   nil,
	}, nil
}

func (persister Persister) GetID() (id uint64, err error) {
	for trys := 5; trys > 0; trys-- {
		id, err = persister.Generator.GenOne()
		if err != nil {
			persister.Log.Error(err.Error())
			continue
		}
		return
	}
	persister.Log.Error("No ID generated.")
	return
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
