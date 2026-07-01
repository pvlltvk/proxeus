package mantineui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/mantine-ui
var embedded embed.FS

// Assets serves promxy's own copy of the built Mantine UI. The filesystem is
// rooted at the mantine-ui directory so Assets.Open("/index.html") and
// Assets.Open("/assets/<file>") resolve — matching the paths the injected
// index.html references (see buildInjectedReactApp). Assets are always
// embedded (no build tag) so every promxy build ships a working UI and never
// depends on Prometheus's web/ui asset path.
var Assets http.FileSystem = http.FS(mustSub())

func mustSub() fs.FS {
	sub, err := fs.Sub(embedded, "static/mantine-ui")
	if err != nil {
		panic(err)
	}
	return sub
}
