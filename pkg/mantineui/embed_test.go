package mantineui

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAssetsRootedAtMantineUI verifies the embedded FS is rooted at the
// mantine-ui directory so the paths the injected index.html references resolve:
// "/index.html" and "/assets/<file>".
func TestAssetsRootedAtMantineUI(t *testing.T) {
	f, err := Assets.Open("/index.html")
	if err != nil {
		t.Fatalf("Assets.Open(/index.html): %v", err)
	}
	f.Close()

	// The build always emits at least one hashed asset under /assets.
	sub, err := fs.Sub(embedded, "static/mantine-ui/assets")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := fs.ReadDir(sub, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no files under static/mantine-ui/assets — UI build missing?")
	}
	asset := entries[0].Name()
	af, err := Assets.Open("/assets/" + asset)
	if err != nil {
		t.Fatalf("Assets.Open(/assets/%s): %v", asset, err)
	}
	af.Close()

	// Simulate the main.go route: http.StripPrefix(routePrefix, FileServer)
	// serving "<prefix>/assets/<file>". index.html is served separately via
	// Assets.Open (buildInjectedReactApp), verified above — FileServer 301s
	// "/index.html" to "./", so it is not exercised through this route.
	const prefix = "/promxytest"
	h := http.StripPrefix(prefix, http.FileServer(Assets))

	for _, tc := range []struct {
		path string
		want int
	}{
		{prefix + "/assets/" + asset, http.StatusOK},
		{prefix + "/assets/does-not-exist.js", http.StatusNotFound},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != tc.want {
			t.Errorf("GET %s: got %d, want %d", tc.path, rec.Code, tc.want)
		}
		if tc.want == http.StatusOK {
			body, _ := io.ReadAll(rec.Body)
			if len(body) == 0 {
				t.Errorf("GET %s: empty body", tc.path)
			}
			if strings.HasSuffix(tc.path, ".js") && rec.Header().Get("Content-Type") == "" {
				t.Errorf("GET %s: no Content-Type", tc.path)
			}
		}
	}
}
