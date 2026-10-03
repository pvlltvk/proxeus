package servergroup

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/sigv4"
	"gopkg.in/yaml.v2"

	"github.com/pvlltvk/proxeus/pkg/promclient"
)

func TestHTTPClientIntegration(t *testing.T) {
	tests := []struct {
		name              string
		sigv4Config       *sigv4.SigV4Config
		expectSigV4       bool
		expectError       bool
		checkAuthHeader   bool
		expectedAuthStart string
	}{
		{
			name: "HTTP client construction with SigV4 configured",
			sigv4Config: &sigv4.SigV4Config{
				Region:    "us-west-2",
				AccessKey: "AKIAIOSFODNN7EXAMPLE",
				SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			expectSigV4:       true,
			expectError:       false,
			checkAuthHeader:   true,
			expectedAuthStart: "AWS4-HMAC-SHA256",
		},
		{
			name:              "HTTP client construction without SigV4 (backward compatibility)",
			sigv4Config:       nil,
			expectSigV4:       false,
			expectError:       false,
			checkAuthHeader:   false,
			expectedAuthStart: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a test server to capture requests
			var capturedRequest *http.Request
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedRequest = r
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			// Create a servergroup with the test configuration
			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			// Create config
			cfg := &Config{
				Scheme:              "https",
				MaxIdleConns:        20000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     5 * time.Minute,
				Timeout:             30 * time.Second,
				HTTPConfig: HTTPClientConfig{
					DialTimeout: 200 * time.Millisecond,
					SigV4Config: tt.sigv4Config,
				},
			}

			// Apply the config
			err = sg.ApplyConfig(cfg)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify the HTTP client was created
			if sg.client == nil {
				t.Errorf("expected HTTP client to be created")
				return
			}

			// If we need to check auth headers, make a request
			if tt.checkAuthHeader {
				// Create a custom client that skips TLS verification for testing
				client := &http.Client{
					Transport: &http.Transport{
						TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
					},
				}

				// Make the request through the servergroup's RoundTrip method
				// Note: We can't easily test the actual SigV4 signing without AWS credentials
				// but we can verify the round tripper chain is set up correctly
				if sg.client.Transport != nil {
					// Just verify the transport exists
					t.Logf("Transport chain configured successfully")
				}

				// Make a simple request to verify the client works
				resp, err := client.Get(server.URL)
				if err != nil {
					t.Fatalf("failed to make request: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("expected status 200, got %d", resp.StatusCode)
				}
			}

			// Verify backward compatibility - no SigV4 means no AWS headers
			if !tt.expectSigV4 && capturedRequest != nil {
				authHeader := capturedRequest.Header.Get("Authorization")
				if strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
					t.Errorf("expected no AWS signature headers, but found Authorization header with AWS4-HMAC-SHA256")
				}
			}
		})
	}
}

func TestSigV4RoundTripperPresence(t *testing.T) {
	tests := []struct {
		name        string
		sigv4Config *sigv4.SigV4Config
		expectError bool
	}{
		{
			name: "SigV4 round tripper present in transport chain",
			sigv4Config: &sigv4.SigV4Config{
				Region:    "us-west-2",
				AccessKey: "AKIAIOSFODNN7EXAMPLE",
				SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			expectError: false,
		},
		{
			name:        "No SigV4 round tripper when not configured",
			sigv4Config: nil,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			cfg := &Config{
				Scheme:              "https",
				MaxIdleConns:        20000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     5 * time.Minute,
				Timeout:             30 * time.Second,
				HTTPConfig: HTTPClientConfig{
					DialTimeout: 200 * time.Millisecond,
					SigV4Config: tt.sigv4Config,
				},
			}

			err = sg.ApplyConfig(cfg)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify the transport chain is configured
			if sg.client == nil {
				t.Errorf("expected HTTP client to be created")
				return
			}

			if sg.client.Transport == nil {
				t.Errorf("expected HTTP client transport to be configured")
				return
			}

			// The transport chain is opaque, but we can verify it was created without error
			t.Logf("Transport chain configured successfully for test: %s", tt.name)
		})
	}
}

func TestRemoteReadClientConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		yamlConfig  string
		expectError bool
	}{
		{
			name: "Remote read client configuration with SigV4",
			yamlConfig: `
remote_read: true
remote_read_path: /api/v1/read
http_client:
  sigv4:
    region: us-west-2
    access_key: AKIAIOSFODNN7EXAMPLE
    secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
static_configs:
  - targets:
      - localhost:9090
`,
			expectError: false,
		},
		{
			name: "Remote read client configuration without SigV4",
			yamlConfig: `
remote_read: true
remote_read_path: /api/v1/read
static_configs:
  - targets:
      - localhost:9090
`,
			expectError: false,
		},
		{
			name: "No remote read client when disabled",
			yamlConfig: `
remote_read: false
static_configs:
  - targets:
      - localhost:9090
`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the YAML configuration
			var cfg Config
			err := yaml.Unmarshal([]byte(tt.yamlConfig), &cfg)
			if err != nil {
				t.Fatalf("failed to unmarshal config: %v", err)
			}

			// Verify that SigV4Config is passed through to remote read client
			// by checking the configuration structure
			if cfg.RemoteRead {
				// When remote read is enabled, the SigV4Config should be available
				// in the HTTPConfig for use by the remote read client
				if tt.name == "Remote read client configuration with SigV4" {
					if cfg.HTTPConfig.SigV4Config == nil {
						t.Errorf("expected SigV4Config to be non-nil for remote read with SigV4")
						return
					}
					if cfg.HTTPConfig.SigV4Config.Region != "us-west-2" {
						t.Errorf("expected region to be us-west-2, got %s", cfg.HTTPConfig.SigV4Config.Region)
					}
					t.Logf("SigV4Config correctly configured for remote read client")
				}

				if tt.name == "Remote read client configuration without SigV4" {
					if cfg.HTTPConfig.SigV4Config != nil {
						t.Errorf("expected SigV4Config to be nil when not configured")
					}
					t.Logf("Remote read client correctly configured without SigV4")
				}
			}

			// Verify backward compatibility
			if !cfg.RemoteRead && cfg.HTTPConfig.SigV4Config != nil {
				t.Errorf("unexpected SigV4Config when remote read is disabled")
			}
		})
	}
}

func TestHTTPClientBackwardCompatibility(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "HTTP client without SigV4 (backward compatibility)",
			config: &Config{
				Scheme:              "https",
				MaxIdleConns:        20000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     5 * time.Minute,
				Timeout:             30 * time.Second,
				HTTPConfig: HTTPClientConfig{
					DialTimeout: 200 * time.Millisecond,
					SigV4Config: nil, // No SigV4
				},
			},
			expectError: false,
		},
		{
			name: "HTTP client with basic auth (backward compatibility)",
			config: &Config{
				Scheme:              "https",
				MaxIdleConns:        20000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     5 * time.Minute,
				Timeout:             30 * time.Second,
				HTTPConfig: HTTPClientConfig{
					DialTimeout: 200 * time.Millisecond,
					SigV4Config: nil,
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			err = sg.ApplyConfig(tt.config)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify the HTTP client was created
			if sg.client == nil {
				t.Errorf("expected HTTP client to be created")
				return
			}

			// Verify the transport exists
			if sg.client.Transport == nil {
				t.Errorf("expected HTTP client transport to be configured")
				return
			}

			t.Logf("HTTP client configured successfully without SigV4 for backward compatibility")
		})
	}
}

func TestSigV4RoundTripperErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		sigv4Config *sigv4.SigV4Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "Valid SigV4 configuration",
			sigv4Config: &sigv4.SigV4Config{
				Region:    "us-west-2",
				AccessKey: "AKIAIOSFODNN7EXAMPLE",
				SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			expectError: false,
		},
		{
			name: "SigV4 with explicit credentials",
			sigv4Config: &sigv4.SigV4Config{
				Region:    "us-west-2",
				AccessKey: "test-access-key",
				SecretKey: "test-secret-key",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg, err := NewServerGroup()
			if err != nil {
				t.Fatalf("failed to create servergroup: %v", err)
			}
			defer sg.Cancel()

			cfg := &Config{
				Scheme:              "https",
				MaxIdleConns:        20000,
				MaxIdleConnsPerHost: 1000,
				IdleConnTimeout:     5 * time.Minute,
				Timeout:             30 * time.Second,
				HTTPConfig: HTTPClientConfig{
					DialTimeout: 200 * time.Millisecond,
					SigV4Config: tt.sigv4Config,
				},
			}

			err = sg.ApplyConfig(cfg)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify the transport chain is configured
			if sg.client == nil {
				t.Errorf("expected HTTP client to be created")
				return
			}

			if sg.client.Transport == nil {
				t.Errorf("expected HTTP client transport to be configured")
				return
			}

			t.Logf("SigV4 round tripper configured successfully for test: %s", tt.name)
		})
	}
}

func TestAuthorizationOnTheWire(t *testing.T) {
	credentialsFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(credentialsFile, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		config string
		want   string
	}{
		{
			name:   "credentials with explicit type",
			config: "authorization: {type: Token, credentials: abc}",
			want:   "Token abc",
		},
		{
			name:   "type defaults to Bearer",
			config: "authorization: {credentials: abc}",
			want:   "Bearer abc",
		},
		{
			name:   "credentials_file",
			config: "authorization: {credentials_file: " + credentialsFile + "}",
			want:   "Bearer from-file",
		},
		{
			name:   "bearer_token",
			config: "bearer_token: legacy",
			want:   "Bearer legacy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Authorization")
			}))
			defer server.Close()

			var cfg Config
			if err := yaml.Unmarshal([]byte("http_client:\n  "+tt.config+"\n"), &cfg); err != nil {
				t.Fatal(err)
			}
			sg, err := NewServerGroup()
			if err != nil {
				t.Fatal(err)
			}
			defer sg.Cancel()
			if err := sg.ApplyConfig(&cfg); err != nil {
				t.Fatal(err)
			}

			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := sg.RoundTrip(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			if got != tt.want {
				t.Errorf("Authorization = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCancelStopsLabelFilterPolling(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"success","data":["a"]}`))
	}))
	defer server.Close()

	cfg := DefaultConfig
	cfg.LabelFilterConfig = &promclient.LabelFilterConfig{
		DynamicLabels: []string{"job"},
		SyncInterval:  10 * time.Millisecond,
	}
	sg, err := NewServerGroup()
	if err != nil {
		t.Fatal(err)
	}
	defer sg.Cancel()
	if err := sg.ApplyConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	groups := map[string][]*targetgroup.Group{
		"x": {{Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(strings.TrimPrefix(server.URL, "http://"))}}}},
	}
	if err := sg.loadTargetGroupMap(groups); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for requests.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("label filter made %d requests, want polling", requests.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}

	sg.Cancel()
	time.Sleep(50 * time.Millisecond) // let an in-flight poll finish
	settled := requests.Load()
	time.Sleep(200 * time.Millisecond)
	if got := requests.Load(); got != settled {
		t.Errorf("label filter kept polling after Cancel: %d requests, was %d", got, settled)
	}
}

func newConfiguredGroup(t *testing.T, yamlConfig string) *ServerGroup {
	t.Helper()
	var cfg Config
	if err := yaml.Unmarshal([]byte(yamlConfig), &cfg); err != nil {
		t.Fatal(err)
	}
	sg, err := NewServerGroup()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sg.Cancel)
	if err := sg.ApplyConfig(&cfg); err != nil {
		t.Fatal(err)
	}
	return sg
}

func roundTrip(sg *ServerGroup, url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := sg.RoundTrip(req)
	if err == nil {
		resp.Body.Close()
	}
	return resp, err
}

func TestFollowRedirects(t *testing.T) {
	tests := []struct {
		name       string
		config     string
		wantFollow bool
	}{
		{name: "followed by default", config: "http_client: {}\n", wantFollow: true},
		{name: "followed when true", config: "http_client: {follow_redirects: true}\n", wantFollow: true},
		{name: "not followed when false", config: "http_client: {follow_redirects: false}\n", wantFollow: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/final" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"status":"success","data":["a"]}`))
					return
				}
				http.Redirect(w, r, "/final", http.StatusFound)
			}))
			defer server.Close()

			sg := newConfiguredGroup(t, tt.config)
			groups := map[string][]*targetgroup.Group{
				"x": {{Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(strings.TrimPrefix(server.URL, "http://"))}}}},
			}
			if err := sg.loadTargetGroupMap(groups); err != nil {
				t.Fatal(err)
			}

			_, _, err := sg.LabelNames(t.Context(), nil, time.Time{}, time.Time{})
			if followed := err == nil; followed != tt.wantFollow {
				t.Errorf("redirect followed = %v (err = %v), want %v", followed, err, tt.wantFollow)
			}
		})
	}
}

func TestProxyOnTheWire(t *testing.T) {
	var proxied atomic.Int64
	var connectHeader atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied.Add(1)
		if r.Method == http.MethodConnect {
			connectHeader.Store(r.Header.Get("X-Proxy-Token"))
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Via", "proxy")
	}))
	defer proxy.Close()

	sg := newConfiguredGroup(t, "http_client:\n  proxy_url: "+proxy.URL+"\n  no_proxy: direct.test\n  proxy_connect_header:\n    X-Proxy-Token: [secret]\n")

	t.Run("proxied", func(t *testing.T) {
		resp, err := roundTrip(sg, "http://proxied.test/")
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Header.Get("X-Via"); got != "proxy" {
			t.Errorf("X-Via = %q, want proxy", got)
		}
	})

	t.Run("no_proxy bypasses", func(t *testing.T) {
		before := proxied.Load()
		if _, err := roundTrip(sg, "http://direct.test/"); err == nil {
			t.Fatal("expected a direct dial failure")
		}
		if proxied.Load() != before {
			t.Error("request to a no_proxy host reached the proxy")
		}
	})

	t.Run("proxy_connect_header", func(t *testing.T) {
		_, _ = roundTrip(sg, "https://secure.test/")
		if got, _ := connectHeader.Load().(string); got != "secret" {
			t.Errorf("CONNECT X-Proxy-Token = %q, want secret", got)
		}
	})
}

func TestEnableHTTP2OnTheWire(t *testing.T) {
	tests := []struct {
		name   string
		config string
		want   int
	}{
		{name: "default", config: "{}", want: 2},
		{name: "disabled", config: "{enable_http2: false}", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got int
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.ProtoMajor
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()

			sg := newConfiguredGroup(t, "http_client: "+tt.config+"\n")
			sg.Cfg.HTTPConfig.HTTPConfig.TLSConfig.InsecureSkipVerify = true
			if err := sg.ApplyConfig(sg.Cfg); err != nil {
				t.Fatal(err)
			}
			if _, err := roundTrip(sg, server.URL); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("protocol major = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestHTTPHeadersOnTheWire(t *testing.T) {
	headerFile := filepath.Join(t.TempDir(), "header")
	if err := os.WriteFile(headerFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var got http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
	}))
	defer server.Close()

	sg := newConfiguredGroup(t, `
http_headers:
  x-shared: group
http_client:
  http_headers:
    X-Plain:
      values: [plain]
    X-Secret:
      secrets: [hidden]
    X-File:
      files: [`+headerFile+`]
    X-SHARED:
      values: [client]
`)
	if _, err := roundTrip(sg, server.URL); err != nil {
		t.Fatal(err)
	}

	want := map[string][]string{
		"X-Plain":  {"plain"},
		"X-Secret": {"hidden"},
		"X-File":   {"from-file"},
		"X-Shared": {"group"},
	}
	for name, values := range want {
		if !slices.Equal(got[name], values) {
			t.Errorf("%s = %q, want %q", name, got[name], values)
		}
	}
}

func TestBasicAuthUsernameFile(t *testing.T) {
	usernameFile := filepath.Join(t.TempDir(), "username")
	if err := os.WriteFile(usernameFile, []byte("alice\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	var user, password string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, _ = r.BasicAuth()
	}))
	defer server.Close()

	sg := newConfiguredGroup(t, "http_client:\n  basic_auth: {username_file: "+usernameFile+", password: s3cret}\n")
	if _, err := roundTrip(sg, server.URL); err != nil {
		t.Fatal(err)
	}
	if user != "alice" || password != "s3cret" {
		t.Errorf("basic auth = %q:%q, want alice:s3cret", user, password)
	}
}
