package main

import (
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/util/notifications"
)

// hasNotification reports whether an active notification with the given text
// is currently published.
func hasNotification(n *notifications.Notifications, text string) bool {
	for _, notif := range n.Get() {
		if notif.Text == text && notif.Active {
			return true
		}
	}
	return false
}

// A failed reload must surface in the UI's notification feed, and a later
// successful reload must clear it again. This also pins that reloadConfig
// never sees a nil *Notifications — the nil getter/subscriber is what made
// /api/v1/notifications panic before the handles were wired up.
func TestReloadConfigNotifications(t *testing.T) {
	notifs := notifications.NewNotifications(16, nil)
	noStepSubqueryInterval := &safePromQLNoStepSubqueryInterval{}

	origConfigFile := opts.ConfigFile
	t.Cleanup(func() { opts.ConfigFile = origConfigFile })

	opts.ConfigFile = filepath.Join(t.TempDir(), "does-not-exist.yaml")
	if err := reloadConfig(noStepSubqueryInterval, notifs); err == nil {
		t.Fatal("reloadConfig succeeded for a missing config file, want error")
	}
	if !hasNotification(notifs, notifications.ConfigurationUnsuccessful) {
		t.Errorf("after a failed reload, %q notification is missing", notifications.ConfigurationUnsuccessful)
	}

	opts.ConfigFile = "local_test.conf"
	if err := reloadConfig(noStepSubqueryInterval, notifs); err != nil {
		t.Fatalf("reloadConfig(%q): %v", opts.ConfigFile, err)
	}
	if hasNotification(notifs, notifications.ConfigurationUnsuccessful) {
		t.Errorf("after a successful reload, %q notification was not cleared", notifications.ConfigurationUnsuccessful)
	}
}

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
		{name: "root unknown", routePrefix: "/", urlPath: "/proxeus/backends", want: false},
		{name: "empty prefix query", routePrefix: "", urlPath: "/query", want: true},

		// Sub-path prefix: paths arrive prefixed with "/foo".
		{name: "foo query", routePrefix: "/foo", urlPath: "/foo/query", want: true},
		{name: "foo backends", routePrefix: "/foo", urlPath: "/foo/backends", want: true},
		// Unprefixed path under a sub-path prefix must NOT match.
		{name: "foo unprefixed query", routePrefix: "/foo", urlPath: "/query", want: false},
		// Non-react path under prefix.
		{name: "foo proxeus", routePrefix: "/foo", urlPath: "/foo/proxeus/backends", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isReactRoute(tc.routePrefix, tc.urlPath); got != tc.want {
				t.Errorf("isReactRoute(%q, %q) = %v, want %v", tc.routePrefix, tc.urlPath, got, tc.want)
			}
		})
	}
}
