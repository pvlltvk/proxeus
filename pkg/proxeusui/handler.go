// Package proxeusui provides the proxeus-specific inventory UI.
//
// It exposes the following routes under the /proxeus/ prefix:
//
//	GET /proxeus/                  — 302 redirect to /proxeus/backends
//	GET /proxeus/backends          — HTML inventory page (rendered from templates/inventory.html)
//	GET /proxeus/api/backends.json — JSON snapshot of all server_groups and targets
//	GET /proxeus/static/*          — static assets owned by the frontend-developer agent
package proxeusui

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/pvlltvk/proxeus/pkg/proxystorage"
)

// Handler serves the proxeus inventory UI.
type Handler struct {
	prober *Prober
	tmpl   *template.Template

	// routePrefix is the -web.route-prefix the UI is served under (e.g. "/" or
	// "/foo"). Used to build absolute redirect targets that honor the prefix,
	// and to strip incoming requests down to the prefix-agnostic patterns
	// registered in mux.
	routePrefix string

	// inventoryFn, when non-nil, overrides prober.Inventory(). Used in tests.
	inventoryFn func() Inventory

	muxOnce sync.Once
	mux     *http.ServeMux
}

// NewHandler constructs a Handler. ps is used to initialize the background
// Prober which health-checks every (group, target) every 30 s. routePrefix is
// the -web.route-prefix the UI is served under and is used to build redirect
// targets that honor the prefix.
func NewHandler(ps *proxystorage.ProxyStorage, routePrefix string) (*Handler, error) {
	tmpl, err := template.ParseFS(Templates, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Handler{
		prober:      NewProber(ps),
		tmpl:        tmpl,
		routePrefix: routePrefix,
	}, nil
}

// Run starts the background health-probe loop. It blocks until ctx is canceled.
// Intended usage: go h.Run(ctx).
func (h *Handler) Run(ctx context.Context) {
	h.prober.Run(ctx)
}

// ServeHTTP routes the request through mux after stripping routePrefix, so
// the patterns registered in initMux stay prefix-agnostic regardless of
// -web.route-prefix.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.muxOnce.Do(h.initMux)

	p := strings.TrimPrefix(r.URL.Path, strings.TrimRight(h.routePrefix, "/"))
	if p == r.URL.Path {
		h.mux.ServeHTTP(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = p
	h.mux.ServeHTTP(w, r2)
}

// initMux registers the routes served under /proxeus/. Patterns are
// prefix-agnostic — ServeHTTP strips -web.route-prefix before dispatch — and
// the redirectToBackends handler reads h.routePrefix at request time to build
// its Location header.
func (h *Handler) initMux() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /proxeus/api/backends.json", h.serveJSON)
	mux.HandleFunc("GET /proxeus/static/", h.serveStatic)
	mux.HandleFunc("GET /proxeus/backends", h.serveIndex)
	mux.HandleFunc("GET /proxeus/backends/", h.serveIndex)
	// Exact "/proxeus" and the "/proxeus/" subtree (everything else under
	// /proxeus/ not matched above) both redirect to the backends page.
	mux.HandleFunc("GET /proxeus", h.redirectToBackends)
	mux.HandleFunc("GET /proxeus/", h.redirectToBackends)
	h.mux = mux
}

// redirectToBackends sends /proxeus/ and any other unmatched /proxeus/* path to
// the backends page, honoring the route prefix (path.Join cleans "" →
// "/proxeus/backends").
func (h *Handler) redirectToBackends(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, path.Join(h.routePrefix, "/proxeus/backends"), http.StatusFound)
}

func (h *Handler) inventory() Inventory {
	if h.inventoryFn != nil {
		return h.inventoryFn()
	}
	return h.prober.Inventory()
}

func (h *Handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	inv := h.inventory()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpl.ExecuteTemplate(w, "inventory.html", inv); err != nil {
		logrus.WithError(err).Error("proxeusui: error rendering index template")
	}
}

func (h *Handler) serveJSON(w http.ResponseWriter, r *http.Request) {
	inv := h.inventory()

	resp := Inventory{
		GeneratedAt: inv.GeneratedAt.UTC(),
		Groups:      make([]GroupInfo, 0, len(inv.Groups)),
	}
	for _, g := range inv.Groups {
		jg := GroupInfo{
			Name:    g.Name,
			Ordinal: g.Ordinal,
			Labels:  g.Labels,
			Targets: make([]TargetInfo, 0, len(g.Targets)),
		}
		if jg.Labels == nil {
			jg.Labels = map[string]string{}
		}
		for _, t := range g.Targets {
			t.LastProbeAt = t.LastProbeAt.UTC()
			jg.Targets = append(jg.Targets, t)
		}
		resp.Groups = append(resp.Groups, jg)
	}

	b, err := json.Marshal(resp)
	if err != nil {
		logrus.WithError(err).Error("proxeusui: error marshaling backends JSON")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(b); err != nil {
		logrus.WithError(err).Debug("proxeusui: error writing backends JSON response")
	}
}

func (h *Handler) serveStatic(w http.ResponseWriter, r *http.Request) {
	// Strip up to and including "/proxeus/" to get the path within the embed.
	sub := http.FileServer(http.FS(StaticFiles))
	// Rewrite: /proxeus/static/foo.css → static/foo.css inside embed FS.
	idx := strings.Index(r.URL.Path, "/static/")
	if idx < 0 {
		http.NotFound(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = r.URL.Path[idx:]
	sub.ServeHTTP(w, r2)
}
