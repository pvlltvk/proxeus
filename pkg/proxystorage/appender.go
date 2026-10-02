package proxystorage

import (
	"errors"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
)

var errReadOnly = errors.New("proxeus is read-only")

type readOnlyAppender struct{}

func (readOnlyAppender) Append(storage.SeriesRef, labels.Labels, int64, float64) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) AppendExemplar(storage.SeriesRef, labels.Labels, exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) AppendHistogram(storage.SeriesRef, labels.Labels, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) AppendHistogramCTZeroSample(storage.SeriesRef, labels.Labels, int64, int64, *histogram.Histogram, *histogram.FloatHistogram) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) AppendCTZeroSample(storage.SeriesRef, labels.Labels, int64, int64) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) UpdateMetadata(storage.SeriesRef, labels.Labels, metadata.Metadata) (storage.SeriesRef, error) {
	return 0, errReadOnly
}

func (readOnlyAppender) SetOptions(*storage.AppendOptions) {}

func (readOnlyAppender) Commit() error { return nil }

func (readOnlyAppender) Rollback() error { return nil }
