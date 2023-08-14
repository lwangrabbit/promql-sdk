package promql_sdk

import (
	"context"
	"errors"
	"time"

	"github.com/lwangrabbit/promql-sdk/util/stats"
)

type InstantQuery struct {
	Query   string
	Ts      int64
	Timeout time.Duration
}

func NewInstantQuery(query string, opts ...func(*InstantQuery)) *InstantQuery {
	q := &InstantQuery{
		Query:   query,
		Ts:      time.Now().Unix(),
		Timeout: DefaultAPIInstantQueryTimeout,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func InstantQueryTime(ts int64) func(query *InstantQuery) {
	return func(query *InstantQuery) {
		query.Ts = ts
	}
}

func InstantQueryTimeout(timeout time.Duration) func(query *InstantQuery) {
	return func(query *InstantQuery) {
		query.Timeout = timeout
	}
}

func (q *InstantQuery) Do() (*QueryData, error) {
	qry, err := queryEngine.NewInstantQuery(remoteStorage, q.Query, time.Unix(q.Ts, 0))
	if err != nil {
		return nil, err
	}
	defer qry.Close()

	ctx, cancel := context.WithTimeout(context.Background(), q.Timeout)
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

type RangeQuery struct {
	Query  string
	Start  int64
	End    int64
	Step   int
	Timout time.Duration
}

func NewRangeQuery(query string, start, end int64, step int, opts ...func(*RangeQuery)) *RangeQuery {
	q := &RangeQuery{
		Query:  query,
		Start:  start,
		End:    end,
		Step:   step,
		Timout: DefaultAPIRangeQueryTimeout,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func RangeQueryTimeout(timeout time.Duration) func(*RangeQuery) {
	return func(query *RangeQuery) {
		query.Timout = timeout
	}
}

func (q *RangeQuery) Do() (*QueryData, error) {
	if q.Start > q.End {
		return nil, errors.New("startTs/endTs error")
	}
	if q.Step <= 0 {
		q.Step = 60
	}
	if (q.End-q.Start)/int64(q.Step) > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries")
		return nil, err
	}

	qry, err := queryEngine.NewRangeQuery(
		remoteStorage,
		q.Query,
		time.Unix(q.Start, 0),
		time.Unix(q.End, 0),
		time.Duration(q.Step)*time.Second)
	if err != nil {
		return nil, err
	}
	defer qry.Close()

	ctx, cancel := context.WithTimeout(context.Background(), q.Timout)
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
