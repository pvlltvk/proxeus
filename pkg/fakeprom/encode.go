package fakeprom

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// flushThreshold is how much response body is buffered before it is handed to
// the (possibly gzipping) writer. Keeping it small is the point: a response
// carrying millions of samples is streamed, never materialized.
const flushThreshold = 32 << 10

// encoder appends response JSON into a scratch buffer and flushes it to the
// client in flushThreshold-sized chunks. Encoders (and their gzip writers,
// which allocate hundreds of KiB apiece) are pooled — at the request rates
// fakeprom is built to sustain, allocating one per request is the difference
// between the load generator and the system under test being the bottleneck.
type encoder struct {
	dst    io.Writer
	gz     *gzip.Writer
	gzipOn bool
	buf    []byte
}

var encoderPool = sync.Pool{
	New: func() any { return &encoder{buf: make([]byte, 0, flushThreshold+4096)} },
}

// newEncoder writes the response headers and returns an encoder for the body,
// gzipping if the client asked for it. The caller must call close.
func newEncoder(w http.ResponseWriter, r *http.Request) *encoder {
	w.Header().Set("Content-Type", "application/json")
	e := encoderPool.Get().(*encoder)
	e.buf = e.buf[:0]
	e.gzipOn = acceptsGzip(r)
	if !e.gzipOn {
		e.dst = w
		return e
	}
	w.Header().Set("Content-Encoding", "gzip")
	if e.gz == nil {
		// BestSpeed: fakeprom's job is to feed the client as fast as it can,
		// not to compress well.
		e.gz, _ = gzip.NewWriterLevel(w, gzip.BestSpeed)
	} else {
		e.gz.Reset(w)
	}
	e.dst = e.gz
	return e
}

func (e *encoder) close() {
	e.flush()
	if e.gzipOn {
		_ = e.gz.Close()
	}
	e.dst = nil
	encoderPool.Put(e)
}

func (e *encoder) writeString(s string) {
	e.buf = append(e.buf, s...)
	e.maybeFlush()
}

func (e *encoder) maybeFlush() {
	if len(e.buf) >= flushThreshold {
		e.flush()
	}
}

// flush writes out what is buffered. Write errors are dropped: they mean the
// client went away, and there is nothing useful to report at that point.
func (e *encoder) flush() {
	if len(e.buf) > 0 {
		_, _ = e.dst.Write(e.buf)
		e.buf = e.buf[:0]
	}
}

func acceptsGzip(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
}

// writeStrings writes a `{"status":"success","data":["a","b"]}` response, the
// shape of the labels and label-values endpoints.
func writeStrings(w http.ResponseWriter, r *http.Request, values []string) {
	e := newEncoder(w, r)
	defer e.close()
	e.writeString(`{"status":"success","data":[`)
	for i, v := range values {
		if i > 0 {
			e.writeString(",")
		}
		e.buf = strconv.AppendQuote(e.buf, v)
		e.maybeFlush()
	}
	e.writeString(`]}`)
}

func writeAPIError(w http.ResponseWriter, code int, errType, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = fmt.Fprintf(w, `{"status":"error","errorType":%q,"error":%q}`, errType, msg)
}

// prefixedInts returns [prefix0 ... prefixN-1] in lexical order, matching how
// Prometheus sorts label values.
func prefixedInts(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = prefix + strconv.Itoa(i)
	}
	slices.Sort(out)
	return out
}

// parseTimeParam parses a time parameter the way the Prometheus API does:
// either a unix timestamp in (possibly fractional) seconds or RFC3339. A
// missing parameter is 0. Returns milliseconds.
func parseTimeParam(r *http.Request, name string) (int64, error) {
	v := r.FormValue(name)
	if v == "" {
		return 0, nil
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return int64(f * 1000), nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s parameter %q", name, v)
	}
	return t.UnixMilli(), nil
}

// parseDurationParam parses a duration parameter, in seconds ("15", "0.5") or
// as a duration string ("15s", "1m"). Returns milliseconds.
func parseDurationParam(r *http.Request, name string) (int64, error) {
	v := r.FormValue(name)
	if v == "" {
		return 0, nil
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return int64(f * 1000), nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s parameter %q", name, v)
	}
	return d.Milliseconds(), nil
}
