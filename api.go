package promql_sdk

import (
	"fmt"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"

	"github.com/lwangrabbit/promql-sdk/promql"
	"github.com/lwangrabbit/promql-sdk/storage"
	"github.com/lwangrabbit/promql-sdk/util/stats"
)

var (
	queryEngine   *promql.Engine
	remoteStorage storage.Storage
)

const (
	DefaultEngineQueryMaxConcurrency = 20
	DefaultEngineQueryMaxSamples     = 50000000
	DefaultEngineQueryTimeout        = 30 * time.Second

	DefaultAPIInstantQueryTimeout = 10 * time.Second
	DefaultAPIRangeQueryTimeout   = 30 * time.Second
)

type ReadConfig struct {
	URL     string
	Timeout time.Duration
}

func Init(configs []*ReadConfig, ops ...func()) error {
	engineOpts := promql.EngineOpts{
		Reg:           prometheus.DefaultRegisterer,
		MaxConcurrent: DefaultEngineQueryMaxConcurrency,
		MaxSamples:    DefaultEngineQueryMaxSamples,
		Timeout:       DefaultEngineQueryTimeout,
	}
	queryEngine = promql.NewEngine(engineOpts)

	// Custom configs
	for _, op := range ops {
		op()
	}

	var err error
	var rConfs = make([]*storage.RemoteReadConfig, 0, len(configs))
	for _, conf := range configs {
		u, err := url.Parse(conf.URL)
		if err != nil {
			return err
		}
		rconf := &storage.RemoteReadConfig{
			URL:           &config_util.URL{URL: u},
			RemoteTimeout: model.Duration(conf.Timeout),
			Name:          fmt.Sprintf("promql-read-%v", conf.URL),
		}
		rConfs = append(rConfs, rconf)
	}
	remoteStorage, err = storage.NewStorage(rConfs)
	if err != nil {
		return err
	}
	return nil
}

func LookBackDelta(t time.Duration) func() {
	return func() {
		if t >= 1*time.Minute && t <= promql.DefaultLookbackDelta {
			promql.LookbackDelta = t
		} else {
			panic("invalid value of lookbackdelta")
		}
	}
}

func Query(query string) (*QueryData, error) {
	return QueryInstant(query, time.Now().Unix())
}

func QueryInstant(query string, ts int64) (*QueryData, error) {
	qry := NewInstantQuery(query, InstantQueryTime(ts), InstantQueryTimeout(30*time.Second))
	return qry.Do()
}

func QueryRange(query string, startTs int64, endTs int64, step int) (*QueryData, error) {
	qry := NewRangeQuery(query, startTs, endTs, step, RangeQueryTimeout(60*time.Second))
	return qry.Do()
}

type QueryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}
