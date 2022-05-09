package storage

import (
	"context"

	"github.com/lwangrabbit/promql-sdk/pkg/labels"
)

// QueryableClient returns a storage.Queryable which queries the given
// Client to select series sets.
func QueryableClient(c *Client) Queryable {
	return QueryableFunc(func(ctx context.Context, mint, maxt int64) (Querier, error) {
		return &querier{
			ctx:    ctx,
			mint:   mint,
			maxt:   maxt,
			client: c,
		}, nil
	})
}

// querier is an adapter to make a Client usable as a storage.Querier.
type querier struct {
	ctx        context.Context
	mint, maxt int64
	client     *Client
}

// Select implements storage.Querier and uses the given matchers to read series
// sets from the Client.
func (q *querier) Select(p *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	query, err := ToQuery(q.mint, q.maxt, matchers, p)
	if err != nil {
		return nil, err
	}

	res, err := q.client.Read(q.ctx, query)
	if err != nil {
		return nil, err
	}

	return FromQueryResult(res), nil
}

// LabelValues implements storage.Querier and is a noop.
func (q *querier) LabelValues(name string) ([]string, error) {
	// TODO implement?
	return nil, nil
}

// Close implements storage.Querier and is a noop.
func (q *querier) Close() error {
	return nil
}

// RequiredMatchersFilter returns a storage.Queryable which creates a
// requiredMatchersQuerier.
func RequiredMatchersFilter(next Queryable, required []*labels.Matcher) Queryable {
	return QueryableFunc(func(ctx context.Context, mint, maxt int64) (Querier, error) {
		q, err := next.Querier(ctx, mint, maxt)
		if err != nil {
			return nil, err
		}
		return &requiredMatchersQuerier{Querier: q, requiredMatchers: required}, nil
	})
}

// requiredMatchersQuerier wraps a storage.Querier and requires Select() calls
// to match the given labelSet.
type requiredMatchersQuerier struct {
	Querier

	requiredMatchers []*labels.Matcher
}

// Select returns a NoopSeriesSet if the given matchers don't match the label
// set of the requiredMatchersQuerier. Otherwise it'll call the wrapped querier.
func (q requiredMatchersQuerier) Select(p *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	ms := q.requiredMatchers
	for _, m := range matchers {
		for i, r := range ms {
			if m.Type == labels.MatchEqual && m.Name == r.Name && m.Value == r.Value {
				ms = append(ms[:i], ms[i+1:]...)
				break
			}
		}
		if len(ms) == 0 {
			break
		}
	}
	if len(ms) > 0 {
		return NoopSeriesSet(), nil
	}
	return q.Querier.Select(p, matchers...)
}
