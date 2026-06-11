// Package promxyui provides the promxy-specific inventory UI.
//
// It exposes the following routes under the /promxy/ prefix:
//
//	GET /promxy/                  — 302 redirect to /promxy/backends
//	GET /promxy/backends          — HTML inventory page (rendered from templates/inventory.html)
//	GET /promxy/api/backends.json — JSON snapshot of all server_groups and targets
//	GET /promxy/static/*          — static assets owned by the frontend-developer agent
package promxyui

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"path"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/jacksontj/promxy/pkg/proxystorage"
)

// Handler serves the promxy inventory UI.
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

// NewHandler constructs a Handler. ps is used to initialise the background
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

// Run starts the background health-probe loop. It blocks until ctx is cancelled.
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

// initMux registers the routes served under /promxy/. Patterns are
// prefix-agnostic — ServeHTTP strips -web.route-prefix before dispatch — and
// the redirectToBackends handler reads h.routePrefix at request time to build
// its Location header.
func (h *Handler) initMux() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /promxy/api/backends.json", h.serveJSON)
	mux.HandleFunc("GET /promxy/static/", h.serveStatic)
	mux.HandleFunc("GET /promxy/backends", h.serveIndex)
	mux.HandleFunc("GET /promxy/backends/", h.serveIndex)
	// Exact "/promxy" and the "/promxy/" subtree (everything else under
	// /promxy/ not matched above) both redirect to the backends page.
	mux.HandleFunc("GET /promxy", h.redirectToBackends)
	mux.HandleFunc("GET /promxy/", h.redirectToBackends)
	h.mux = mux
}

// redirectToBackends sends /promxy/ and any other unmatched /promxy/* path to
// the backends page, honoring the route prefix (path.Join cleans "" →
// "/promxy/backends").
func (h *Handler) redirectToBackends(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, path.Join(h.routePrefix, "/promxy/backends"), http.StatusFound)
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
		logrus.WithError(err).Error("promxyui: error rendering index template")
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
		logrus.WithError(err).Error("promxyui: error marshaling backends JSON")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(b); err != nil {
		logrus.WithError(err).Debug("promxyui: error writing backends JSON response")
	}
}

func (h *Handler) serveStatic(w http.ResponseWriter, r *http.Request) {
	// Strip up to and including "/promxy/" to get the path within the embed.
	sub := http.FileServer(http.FS(StaticFiles))
	// Rewrite: /promxy/static/foo.css → static/foo.css inside embed FS.
	idx := strings.Index(r.URL.Path, "/static/")
	if idx < 0 {
		http.NotFound(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = r.URL.Path[idx:]
	sub.ServeHTTP(w, r2)
}
