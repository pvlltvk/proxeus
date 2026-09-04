package logging

import (
	"bytes"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

func TestFormPrefix(t *testing.T) {
	tooManyValues := make(url.Values)

	for i := 0; i < MaxFormPrefix; i++ {
		k := strconv.Itoa(i)
		tooManyValues[k] = []string{k}
	}

	tests := []struct {
		f url.Values
		v string
	}{
		// Basic one
		{
			f: url.Values(map[string][]string{
				"a": {"a"},
			}),
			v: `a=a`,
		},
		// where a single value is too long
		{
			f: url.Values(map[string][]string{
				"a": {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			}),
			v: `a=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`,
		},
		// Where there are too many values
		{
			f: tooManyValues,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			v := FormPrefix(test.f)
			if len(v) > MaxFormPrefix {
				t.Fatalf("Value over MaxFormPrefix: expected=%d actual=%d", MaxFormPrefix, len(v))
			}
			if test.v != "" {
				if v != test.v {
					t.Fatalf("Mismatch in values expected=%s actual=%s", test.v, v)
				}
			}
		})
	}
}

// The user field of the access log line is filled in from the request's
// authenticated identity (see pkg/auth); anonymous requests keep the "-" the
// combined log format uses for an absent user.
func TestApacheLogRecordUser(t *testing.T) {
	tests := []struct {
		name string
		user string
		text string
		json string
	}{
		{
			name: "anonymous",
			text: `1.2.3.4 - - [`,
			json: `"remoteAddr":"1.2.3.4"`,
		},
		{
			name: "authenticated",
			user: "alice",
			text: `1.2.3.4 - alice [`,
			json: `"user":"alice"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := &ApacheLogRecord{IP: "1.2.3.4", User: test.user, Method: "GET", URI: "/api/v1/query"}

			var text bytes.Buffer
			record.Log(&text)
			if !strings.HasPrefix(text.String(), test.text) {
				t.Errorf("log line = %q, want it to start with %q", text.String(), test.text)
			}

			var js bytes.Buffer
			record.LogJson(&js)
			if !strings.Contains(js.String(), test.json) {
				t.Errorf("json log line = %q, want it to contain %q", js.String(), test.json)
			}
			if test.user == "" && strings.Contains(js.String(), `"user"`) {
				t.Errorf("json log line = %q, want no user field for an anonymous request", js.String())
			}
		})
	}
}

func TestSetUser(t *testing.T) {
	// The name comes from a token claim or a header, and the text format is
	// positional, so anything that could forge a field or a line is replaced.
	tests := map[string]string{
		"alice":              "alice",
		"alice@example.com":  "alice@example.com",
		"alice smith":        "alice_smith",
		"alice\n1.2.3.4 - -": "alice_1.2.3.4_-_-",
		"alice\tbob":         "alice_bob",
	}

	for user, want := range tests {
		record := &ApacheLogRecord{ResponseWriter: httptest.NewRecorder()}
		SetUser(record, user)
		if record.User != want {
			t.Errorf("SetUser(%q): User = %q, want %q", user, record.User, want)
		}
	}

	// Without access logging the handler chain sees a plain ResponseWriter.
	SetUser(httptest.NewRecorder(), "alice")
}
