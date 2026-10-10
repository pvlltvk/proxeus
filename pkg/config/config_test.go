package proxyconfig

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/common/model"

	"github.com/pvlltvk/proxeus/pkg/servergroup"
)

func TestConfigFromFile(t *testing.T) {
	file, err := os.CreateTemp(os.TempDir(), "")
	if err != nil {
		t.Errorf("Could not create temp file:")
	}

	fileContents := `
tls_server_config:
  cert_file: "server.crt"
  key_file : "server.key"
  client_auth_type : "VerifyClientCertIfGiven"
  client_ca_file : "tls-ca-chain.pem"
`
	file.Write([]byte(fileContents))
	configFilePath := file.Name()

	cfg, err := ConfigFromFile(configFilePath)
	if err != nil {
		t.Errorf("Error was not nil: %+v", err)
	}

	if cfg.WebConfig.TLSCertPath != "server.crt" {
		t.Errorf("Invalid TLSKeypath. Expected 'server.crt', Got '%s'", cfg.WebConfig.TLSCertPath)
	}
	if cfg.WebConfig.TLSKeyPath != "server.key" {
		t.Errorf("Invalid TLSCertPath. Expected 'server.key', Got '%s'", cfg.WebConfig.TLSKeyPath)
	}
	if cfg.WebConfig.ClientAuth != "VerifyClientCertIfGiven" {
		t.Errorf("Invalid ClientAuth. Expected 'VerifyClientCertIfGiven', Got '%s'", cfg.WebConfig.ClientAuth)
	}
	if cfg.WebConfig.ClientCAs != "tls-ca-chain.pem" {
		t.Errorf("Invalid ClientCAs. Expected 'tls-ca-chain.pem', Got '%s'", cfg.WebConfig.ClientCAs)
	}
}

func TestConfigFromBytesRejectsLabelCollision(t *testing.T) {
	const collide = `
proxeus:
  cross_group_dedup: %t
  server_groups:
    - static_configs: [{targets: [a:9090]}]
      labels: {backend: same}
    - static_configs: [{targets: [b:9090]}]
      labels: {backend: same}
`
	if _, err := ConfigFromBytes([]byte(fmt.Sprintf(collide, true))); err == nil {
		t.Error("colliding server_group labels were accepted with cross_group_dedup on")
	}
	// Without dedup a collision is only a provenance concern, and ApplyConfig
	// warns about it -- loading must still succeed.
	if _, err := ConfigFromBytes([]byte(fmt.Sprintf(collide, false))); err != nil {
		t.Errorf("collision without cross_group_dedup was rejected: %v", err)
	}
}

func TestConfigFromBytesValidates(t *testing.T) {
	if _, err := ConfigFromBytes([]byte("proxeus:\n  cross_group_dedup_metadata: true\n")); err == nil {
		t.Error("cross_group_dedup_metadata without cross_group_dedup was accepted")
	}
	if _, err := ConfigFromBytes([]byte("proxeus:\n  cross_group_partial_response: true\n")); err == nil {
		t.Error("cross_group_partial_response without cross_group_dedup was accepted")
	}
	if _, err := ConfigFromBytes([]byte("proxeus:\n  cross_group_dedup: true\n  cross_group_dedup_metadata: true\n")); err != nil {
		t.Errorf("valid config was rejected: %v", err)
	}
}

func TestConfigFromBytesValidatesIgnoreLabels(t *testing.T) {
	if _, err := ConfigFromBytes([]byte(
		"proxeus:\n  cross_group_dedup_ignore_labels: [tenant_id]\n",
	)); err == nil {
		t.Error("cross_group_dedup_ignore_labels without cross_group_dedup was accepted")
	}
	if _, err := ConfigFromBytes([]byte(
		"proxeus:\n  cross_group_dedup: true\n  cross_group_dedup_ignore_labels: [__name__]\n",
	)); err == nil {
		t.Error("cross_group_dedup_ignore_labels containing __name__ was accepted")
	}
	if _, err := ConfigFromBytes([]byte(
		"proxeus:\n  cross_group_dedup: true\n  cross_group_dedup_ignore_labels: [receive_replica, tenant_id]\n",
	)); err != nil {
		t.Errorf("valid cross_group_dedup_ignore_labels config was rejected: %v", err)
	}
}

func TestConfigFromBytesFillGapsDefault(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		want bool
	}{
		{"", true},
		{"proxeus:\n  cross_group_dedup: true\n", true},
		{"proxeus:\n  cross_group_dedup: true\n  cross_group_dedup_fill_gaps: false\n", false},
	} {
		cfg, err := ConfigFromBytes([]byte(tc.yaml))
		if err != nil {
			t.Fatalf("%q: %v", tc.yaml, err)
		}
		if cfg.CrossGroupDedupFillGaps != tc.want {
			t.Errorf("%q: CrossGroupDedupFillGaps = %t, want %t", tc.yaml, cfg.CrossGroupDedupFillGaps, tc.want)
		}
	}
	if _, err := ConfigFromBytes([]byte(
		"proxeus:\n  cross_group_dedup: true\n  cross_group_dedup_fill_gaps: false\n  cross_group_dedup_gap: 30s\n",
	)); err == nil {
		t.Error("cross_group_dedup_gap with fill gaps turned off was accepted")
	}
}

func TestConfigFromBytesRejectsWriteSide(t *testing.T) {
	for _, raw := range []string{
		"rule_files: ['*.rules']\n",
		"remote_write:\n  - url: http://localhost:8083/receive\n",
		"alerting:\n  alertmanagers:\n    - static_configs:\n        - targets: ['am:9093']\n",
	} {
		if _, err := ConfigFromBytes([]byte(raw)); err == nil {
			t.Errorf("%q was accepted", raw)
		}
	}
}

func TestAuthConfig(t *testing.T) {
	cfg, err := ConfigFromBytes([]byte("proxeus:\n  server_groups: []\n"))
	if err != nil {
		t.Fatalf("ConfigFromBytes: %v", err)
	}
	if cfg.Auth != nil {
		t.Fatalf("Auth = %+v, want nil when there is no auth block", cfg.Auth)
	}

	cfg, err = ConfigFromBytes([]byte(`
proxeus:
  auth:
    trusted_header:
      user_header: X-Forwarded-User
      trusted_proxies: [127.0.0.1/32]
`))
	if err != nil {
		t.Fatalf("ConfigFromBytes: %v", err)
	}
	if cfg.Auth == nil || cfg.Auth.TrustedHeader.UserHeader != "X-Forwarded-User" {
		t.Fatalf("Auth = %+v, want the trusted_header provider", cfg.Auth)
	}

	if _, err := ConfigFromBytes([]byte("proxeus:\n  auth: {}\n")); err == nil {
		t.Fatal("an auth block with no provider was accepted")
	}
}

// Config.String() is what /api/v1/status/config serves, so the basic auth
// hashes must not survive the round trip -- they are offline-crackable.
func TestAuthConfigRedactsPasswordHashes(t *testing.T) {
	const hash = "$2a$10$nRYmVvmznzCXqV9O7Bq/beEBbTBlv7GVEt9gyhqiGt.lZdBYcojHK"

	cfg, err := ConfigFromBytes([]byte("proxeus:\n  auth:\n    basic:\n      users:\n        alice: " + hash + "\n"))
	if err != nil {
		t.Fatalf("ConfigFromBytes: %v", err)
	}

	rendered := cfg.String()
	if strings.Contains(rendered, hash) {
		t.Fatalf("rendered config contains the password hash:\n%s", rendered)
	}
	if !strings.Contains(rendered, "<secret>") {
		t.Fatalf("rendered config does not mark the password as a secret:\n%s", rendered)
	}
}

// The cross-group flags only affect the fan-out/dedup machinery, so each one
// requires cross_group_dedup. cross_group_exact especially: without
// dedup the raw fan-out returns both groups' series and the locally computed
// aggregate double-counts exactly as before.
func TestProxeusConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ProxeusConfig
		wantErr bool
	}{
		{name: "nothing enabled", cfg: ProxeusConfig{}},
		{name: "dedup alone", cfg: ProxeusConfig{CrossGroupDedup: true}},
		{name: "exact without dedup", cfg: ProxeusConfig{CrossGroupExact: true}, wantErr: true},
		{name: "exact with dedup", cfg: ProxeusConfig{CrossGroupDedup: true, CrossGroupExact: true}},
		{name: "dedup_metadata without dedup", cfg: ProxeusConfig{CrossGroupDedupMetadata: true}, wantErr: true},
		{name: "partial_response without dedup", cfg: ProxeusConfig{CrossGroupPartialResponse: true}, wantErr: true},
		{
			name:    "ignore_labels without dedup",
			cfg:     ProxeusConfig{CrossGroupDedupIgnoreLabels: []string{"tenant_id"}},
			wantErr: true,
		},
		{
			name: "ignore_labels with dedup",
			cfg:  ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupIgnoreLabels: []string{"receive_replica", "tenant_id"}},
		},
		{
			name:    "ignore_labels rejects __name__",
			cfg:     ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupIgnoreLabels: []string{"__name__"}},
			wantErr: true,
		},
		{
			name:    "ignore_labels rejects empty name",
			cfg:     ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupIgnoreLabels: []string{""}},
			wantErr: true,
		},
		{
			name:    "ignore_labels rejects duplicates",
			cfg:     ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupIgnoreLabels: []string{"tenant_id", "tenant_id"}},
			wantErr: true,
		},
		{
			name:    "ignore_labels rejects invalid label name",
			cfg:     ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupIgnoreLabels: []string{"\xff"}},
			wantErr: true,
		},
		{
			name: "ignore_labels allows a name that overlaps a group labels key",
			cfg: ProxeusConfig{
				CrossGroupDedup:             true,
				CrossGroupDedupIgnoreLabels: []string{"backend"},
				ServerGroups: []*servergroup.Config{
					{Labels: model.LabelSet{"backend": "sg0"}},
					{Labels: model.LabelSet{"backend": "sg1"}},
				},
			},
		},
		{
			name: "fill_gaps without dedup",
			cfg:  ProxeusConfig{CrossGroupDedupFillGaps: true},
		},
		{
			name: "fill_gaps with dedup",
			cfg:  ProxeusConfig{CrossGroupDedup: true, CrossGroupDedupFillGaps: true},
		},
		{
			name: "gap without fill_gaps",
			cfg: ProxeusConfig{
				CrossGroupDedup:    true,
				CrossGroupDedupGap: 30 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "gap with fill_gaps",
			cfg: ProxeusConfig{
				CrossGroupDedup:         true,
				CrossGroupDedupFillGaps: true,
				CrossGroupDedupGap:      30 * time.Second,
			},
		},
		{
			name: "negative gap rejected",
			cfg: ProxeusConfig{
				CrossGroupDedup:         true,
				CrossGroupDedupFillGaps: true,
				CrossGroupDedupGap:      -time.Second,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() = %v, wantErr %t", err, tc.wantErr)
			}
		})
	}
}
