package promql_sdk

import (
	"context"
	"errors"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"

	"github.com/lwangrabbit/promql-sdk/promql"
	"github.com/lwangrabbit/promql-sdk/storage"
	"github.com/lwangrabbit/promql-sdk/util/stats"
)

var (
	queryEngine   *promql.Engine
	remoteStorage storage.Storage

	DefaultQueryMaxConcurrency = 20
	DefaultQueryMaxSamples     = 50000000
	DefaultQueryTimeout        = 2 * time.Minute
)

type ReadConfig struct {
	URL     string
	Timeout time.Duration
}

func Init(conf *ReadConfig) error {
	engineOpts := promql.EngineOpts{
		Logger:        promlog.New(&promlog.Config{}),
		Reg:           prometheus.DefaultRegisterer,
		MaxConcurrent: DefaultQueryMaxConcurrency,
		MaxSamples:    DefaultQueryMaxSamples,
		Timeout:       DefaultQueryTimeout,
	}
	queryEngine = promql.NewEngine(engineOpts)

	var err error
	u, err := url.Parse(conf.URL)
	if err != nil {
		return err
	}
	rconf := &storage.RemoteReadConfig{
		URL:           &config_util.URL{URL: u},
		RemoteTimeout: model.Duration(conf.Timeout),
		Name:          "promql-read",
	}
	remoteStorage, err = storage.NewStorage(rconf)
	if err != nil {
		return err
	}
	return nil
}

func Query(query string) (*QueryData, error) {
	ts := time.Now()
	qry, err := queryEngine.NewInstantQuery(remoteStorage, query, ts)
	if err != nil {
		return nil, err
	}
	defer qry.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return &QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      stats.NewQueryStats(qry.Stats()),
	}, nil
}

func QueryRange(query string, startTs int64, endTs int64, step int) (*QueryData, error) {
	if startTs > endTs {
		return nil, errors.New("startTs/endTs error")
	}
	if step <= 0 {
		step = 60
	}
	if (endTs-startTs)/int64(step) > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries")
		return nil, err
	}

	qry, err := queryEngine.NewRangeQuery(
		remoteStorage,
		query,
		time.Unix(startTs, 0),
		time.Unix(endTs, 0),
		time.Duration(step)*time.Second)
	if err != nil {
		return nil, err
	}
	defer qry.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	res := qry.Exec(ctx)
	if res.Err != nil {
		return nil, res.Err
	}
	return &QueryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
		Stats:      stats.NewQueryStats(qry.Stats()),
	}, nil
}

type QueryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}
