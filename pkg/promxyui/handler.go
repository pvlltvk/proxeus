// Package promxyui provides the promxy-specific inventory UI.
//
// It exposes two routes under the /promxy/ prefix:
//
//	GET /promxy/                  — HTML inventory page (rendered from templates/inventory.html)
//	GET /promxy/api/backends.json — JSON snapshot of all server_groups and targets
//	GET /promxy/static/*          — static assets owned by the frontend-developer agent
package promxyui

import (
	"context"
	"encoding/json"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/jacksontj/promxy/pkg/proxystorage"
)

// jsonTarget is the wire shape for a single target in the JSON feed.
type jsonTarget struct {
	URL         string      `json:"url"`
	Healthy     bool        `json:"healthy"`
	LastError   string      `json:"lastError"`
	LastProbeAt time.Time   `json:"lastProbeAt"`
	BackendType BackendType `json:"backendType"`
	Version     string      `json:"version"`
}

// jsonGroup is the wire shape for a single server_group in the JSON feed.
type jsonGroup struct {
	Name    string            `json:"name"`
	Ordinal int               `json:"ordinal"`
	Labels  map[string]string `json:"labels"`
	Targets []jsonTarget      `json:"targets"`
}

// jsonResponse is the top-level JSON envelope.
type jsonResponse struct {
	GeneratedAt time.Time   `json:"generatedAt"`
	Groups      []jsonGroup `json:"groups"`
}

// Handler serves the promxy inventory UI.
type Handler struct {
	prober *Prober
	tmpl   *template.Template

	// inventoryFn, when non-nil, overrides prober.Inventory(). Used in tests.
	inventoryFn func() Inventory
}

// NewHandler constructs a Handler. ps is used to initialise the background
// Prober which health-checks every (group, target) every 30 s.
func NewHandler(ps *proxystorage.ProxyStorage) (*Handler, error) {
	tmpl, err := template.ParseFS(Templates, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Handler{
		prober: NewProber(ps),
		tmpl:   tmpl,
	}, nil
}

// Run starts the background health-probe loop. It blocks until ctx is cancelled.
// Intended usage: go h.Run(ctx).
func (h *Handler) Run(ctx context.Context) {
	h.prober.Run(ctx)
}

// ServeHTTP dispatches on path suffix.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case strings.HasSuffix(p, "/api/backends.json"):
		h.serveJSON(w, r)
	case strings.Contains(p, "/static/"):
		h.serveStatic(w, r)
	default:
		h.serveIndex(w, r)
	}
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

	resp := jsonResponse{
		GeneratedAt: inv.GeneratedAt.UTC(),
		Groups:      make([]jsonGroup, 0, len(inv.Groups)),
	}
	for _, g := range inv.Groups {
		jg := jsonGroup{
			Name:    g.Name,
			Ordinal: g.Ordinal,
			Labels:  g.Labels,
			Targets: make([]jsonTarget, 0, len(g.Targets)),
		}
		if jg.Labels == nil {
			jg.Labels = map[string]string{}
		}
		for _, t := range g.Targets {
			jg.Targets = append(jg.Targets, jsonTarget{
				URL:         t.URL,
				Healthy:     t.Healthy,
				LastError:   t.LastError,
				LastProbeAt: t.LastProbeAt.UTC(),
				BackendType: t.BackendType,
				Version:     t.Version,
			})
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
