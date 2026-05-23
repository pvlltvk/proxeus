package promxyui

import "embed"

// Templates holds the HTML template files rendered by the UI handler.
// The frontend-developer agent populates templates/*.html.
//
//go:embed templates/*.html
var Templates embed.FS

// StaticFiles holds CSS, JS, and other static assets served under /promxy/static/.
// The frontend-developer agent populates the static/ directory.
//
//go:embed static
var StaticFiles embed.FS
