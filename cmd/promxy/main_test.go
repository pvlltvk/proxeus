package main

import "testing"

func TestIsReactRoute(t *testing.T) {
	cases := []struct {
		name        string
		routePrefix string
		urlPath     string
		want        bool
	}{
		// Root prefix (normalized to "/"): paths arrive unprefixed.
		{name: "root query", routePrefix: "/", urlPath: "/query", want: true},
		{name: "root backends", routePrefix: "/", urlPath: "/backends", want: true},
		{name: "root unknown", routePrefix: "/", urlPath: "/promxy/backends", want: false},
		{name: "empty prefix query", routePrefix: "", urlPath: "/query", want: true},

		// Sub-path prefix: paths arrive prefixed with "/foo".
		{name: "foo query", routePrefix: "/foo", urlPath: "/foo/query", want: true},
		{name: "foo backends", routePrefix: "/foo", urlPath: "/foo/backends", want: true},
		// Unprefixed path under a sub-path prefix must NOT match.
		{name: "foo unprefixed query", routePrefix: "/foo", urlPath: "/query", want: false},
		// Non-react path under prefix.
		{name: "foo promxy", routePrefix: "/foo", urlPath: "/foo/promxy/backends", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isReactRoute(tc.routePrefix, tc.urlPath); got != tc.want {
				t.Errorf("isReactRoute(%q, %q) = %v, want %v", tc.routePrefix, tc.urlPath, got, tc.want)
			}
		})
	}
}
