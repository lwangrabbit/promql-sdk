package storage

import (
	"container/heap"
	"strings"
	"sync"

	"github.com/lwangrabbit/promql-sdk/pkg/labels"
)

// mergeQuerier implements Querier.
type mergeQuerier struct {
	queriers []Querier
}

// NewMergeQuerier returns a new Querier that merges results of input queriers.
// NB NewMergeQuerier will return NoopQuerier if no queriers are passed to it,
// and will filter NoopQueriers from its arguments, in order to reduce overhead
// when only one querier is passed.
func NewMergeQuerier(queriers []Querier) Querier {
	filtered := make([]Querier, 0, len(queriers))
	for _, querier := range queriers {
		if querier != NoopQuerier() {
			filtered = append(filtered, querier)
		}
	}

	switch len(filtered) {
	case 0:
		return NoopQuerier()
	case 1:
		return filtered[0]
	default:
		return &mergeQuerier{
			queriers: filtered,
		}
	}
}

// Select returns a set of series that matches the given label matchers.
func (q *mergeQuerier) Select(params *SelectParams, matchers ...*labels.Matcher) (SeriesSet, error) {
	seriesSets := make([]SeriesSet, 0, len(q.queriers))
	var wg sync.WaitGroup
	seriesSetChan := make(chan SeriesSet, len(q.queriers))
	var errors []error
	for _, querier := range q.queriers {
		wg.Add(1)
		go func(querier Querier) {
			defer wg.Done()

			set, err := querier.Select(params, matchers...)
			if err != nil {
				errors = append(errors, err)
			} else {
				seriesSetChan <- set
			}
		}(querier)
	}

	go func() {
		wg.Wait()
		close(seriesSetChan)
	}()

	for r := range seriesSetChan {
		seriesSets = append(seriesSets, r)
	}

	if len(errors) >= len(q.queriers) {
		return nil, errors[0]
	}
	return NewMergeSeriesSet(seriesSets), nil
}

// LabelValues returns all potential values for a label name.
func (q *mergeQuerier) LabelValues(name string) ([]string, error) {
	var results [][]string
	for _, querier := range q.queriers {
		values, err := querier.LabelValues(name)
		if err != nil {
			return nil, err
		}
		results = append(results, values)
	}
	return mergeStringSlices(results), nil
}

func mergeStringSlices(ss [][]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergeTwoStringSlices(ss[0], ss[1])
	default:
		halfway := len(ss) / 2
		return mergeTwoStringSlices(
			mergeStringSlices(ss[:halfway]),
			mergeStringSlices(ss[halfway:]),
		)
	}
}

func mergeTwoStringSlices(a, b []string) []string {
	i, j := 0, 0
	result := make([]string, 0, len(a)+len(b))
	for i < len(a) && j < len(b) {
		switch strings.Compare(a[i], b[j]) {
		case 0:
			result = append(result, a[i])
			i++
			j++
		case -1:
			result = append(result, a[i])
			i++
		case 1:
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// Close releases the resources of the Querier.
func (q *mergeQuerier) Close() error {
	// TODO return multiple errors?
	var lastErr error
	for _, querier := range q.queriers {
		if err := querier.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// mergeSeriesSet implements SeriesSet
type mergeSeriesSet struct {
	currentLabels labels.Labels
	currentSets   []SeriesSet
	heap          seriesSetHeap
	sets          []SeriesSet
}

// NewMergeSeriesSet returns a new series set that merges (deduplicates)
// series returned by the input series sets when iterating.
func NewMergeSeriesSet(sets []SeriesSet) SeriesSet {
	if len(sets) == 1 {
		return sets[0]
	}

	// Sets need to be pre-advanced, so we can introspect the label of the
	// series under the cursor.
	var h seriesSetHeap
	for _, set := range sets {
		if set.Next() {
			heap.Push(&h, set)
		}
	}
	return &mergeSeriesSet{
		heap: h,
		sets: sets,
	}
}

func (c *mergeSeriesSet) Next() bool {
	// Firstly advance all the current series sets.  If any of them have run out
	// we can drop them, otherwise they should be inserted back into the heap.
	for _, set := range c.currentSets {
		if set.Next() {
			heap.Push(&c.heap, set)
		}
	}
	if len(c.heap) == 0 {
		return false
	}

	// Now, pop items of the heap that have equal label sets.
	c.currentSets = nil
	c.currentLabels = c.heap[0].At().Labels()
	for len(c.heap) > 0 && labels.Equal(c.currentLabels, c.heap[0].At().Labels()) {
		set := heap.Pop(&c.heap).(SeriesSet)
		c.currentSets = append(c.currentSets, set)
	}
	return true
}

func (c *mergeSeriesSet) At() Series {
	if len(c.currentSets) == 1 {
		return c.currentSets[0].At()
	}
	series := []Series{}
	for _, seriesSet := range c.currentSets {
		series = append(series, seriesSet.At())
	}
	return &mergeSeries{
		labels: c.currentLabels,
		series: series,
	}
}

func (c *mergeSeriesSet) Err() error {
	for _, set := range c.sets {
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesSetHeap []SeriesSet

func (h seriesSetHeap) Len() int      { return len(h) }
func (h seriesSetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesSetHeap) Less(i, j int) bool {
	a, b := h[i].At().Labels(), h[j].At().Labels()
	return labels.Compare(a, b) < 0
}

func (h *seriesSetHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesSet))
}

func (h *seriesSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type mergeSeries struct {
	labels labels.Labels
	series []Series
}

func (m *mergeSeries) Labels() labels.Labels {
	return m.labels
}

func (m *mergeSeries) Iterator() SeriesIterator {
	iterators := make([]SeriesIterator, 0, len(m.series))
	for _, s := range m.series {
		iterators = append(iterators, s.Iterator())
	}
	return newMergeIterator(iterators)
}

type mergeIterator struct {
	iterators []SeriesIterator
	h         seriesIteratorHeap
}

func newMergeIterator(iterators []SeriesIterator) SeriesIterator {
	return &mergeIterator{
		iterators: iterators,
		h:         nil,
	}
}

func (c *mergeIterator) Seek(t int64) bool {
	c.h = seriesIteratorHeap{}
	for _, iter := range c.iterators {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		}
	}
	return len(c.h) > 0
}

func (c *mergeIterator) At() (t int64, v float64) {
	if len(c.h) == 0 {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	return c.h[0].At()
}

func (c *mergeIterator) Next() bool {
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}

		return len(c.h) > 0
	}

	if len(c.h) == 0 {
		return false
	}

	currt, _ := c.At()
	for len(c.h) > 0 {
		nextt, _ := c.h[0].At()
		if nextt != currt {
			break
		}

		iter := heap.Pop(&c.h).(SeriesIterator)
		if iter.Next() {
			heap.Push(&c.h, iter)
		}
	}

	return len(c.h) > 0
}

func (c *mergeIterator) Err() error {
	for _, iter := range c.iterators {
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return nil
}

type seriesIteratorHeap []SeriesIterator

func (h seriesIteratorHeap) Len() int      { return len(h) }
func (h seriesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesIteratorHeap) Less(i, j int) bool {
	at, _ := h[i].At()
	bt, _ := h[j].At()
	return at < bt
}

func (h *seriesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesIterator))
}

func (h *seriesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
