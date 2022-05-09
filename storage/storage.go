package storage

import (
	"context"

	"github.com/prometheus/common/model"

	"github.com/lwangrabbit/promql-sdk/pkg/labels"
)

type storage struct {
	queryable Queryable
}

func NewStorage(conf *RemoteReadConfig) (Storage, error) {
	c, err := NewClient(0, &ClientConfig{
		URL:              conf.URL,
		Timeout:          conf.RemoteTimeout,
		HTTPClientConfig: conf.HTTPClientConfig,
	})
	if err != nil {
		return nil, err
	}
	q := QueryableClient(c)
	if len(conf.RequiredMatchers) > 0 {
		q = RequiredMatchersFilter(q, labelsToEqualityMatchers(conf.RequiredMatchers))
	}
	return &storage{
		queryable: q,
	}, nil
}

func (s *storage) ApplyConfig(conf *RemoteReadConfig) error {
	c, err := NewClient(0, &ClientConfig{
		URL:              conf.URL,
		Timeout:          conf.RemoteTimeout,
		HTTPClientConfig: conf.HTTPClientConfig,
	})
	if err != nil {
		return err
	}
	q := QueryableClient(c)
	if len(conf.RequiredMatchers) > 0 {
		q = RequiredMatchersFilter(q, labelsToEqualityMatchers(conf.RequiredMatchers))
	}
	s.queryable = q
	return nil
}

func (s *storage) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return s.queryable.Querier(ctx, mint, maxt)
}

func (s *storage) Close() error {
	return nil
}

func (s *storage) StartTime() (int64, error) {
	return int64(model.Latest), nil
}

func (s *storage) Appender() (Appender, error) {
	return s, nil
}

// Add implements storage.Appender.
func (s *storage) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	return 0, nil
}

// AddFast implements storage.Appender.
func (s *storage) AddFast(l labels.Labels, _ uint64, t int64, v float64) error {
	_, err := s.Add(l, t, v)
	return err
}

// Commit implements storage.Appender.
func (*storage) Commit() error {
	return nil
}

// Rollback implements storage.Appender.
func (*storage) Rollback() error {
	return nil
}

func labelsToEqualityMatchers(ls model.LabelSet) []*labels.Matcher {
	ms := make([]*labels.Matcher, 0, len(ls))
	for k, v := range ls {
		ms = append(ms, &labels.Matcher{
			Type:  labels.MatchEqual,
			Name:  string(k),
			Value: string(v),
		})
	}
	return ms
}
