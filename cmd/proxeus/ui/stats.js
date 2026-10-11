/* Proxeus per-backend query stats for the Mantine query page. */
(function(){
  'use strict';

  var PREFIX = (window.__PROXEUS_ROUTE_PREFIX__ || '');
  var API_PREFIX = PREFIX + '/api/v1/';
  var OPEN_KEY = 'proxeus.backendStats.open';
  var MAX_KEPT = 50;
  var BLOCK_ATTR = 'data-proxeus-backend-stats';

  // Latest `backends` stats per endpoint, keyed by the normalized query.
  var latest = { query: {}, query_range: {} };
  var received = 0;

  function endpointOf(u) {
    if (u.origin !== window.location.origin) return null;
    if (u.pathname === API_PREFIX + 'query') return 'query';
    if (u.pathname === API_PREFIX + 'query_range') return 'query_range';
    return null;
  }

  function norm(q) {
    return String(q).replace(/\s+/g, '');
  }

  function remember(endpoint, query, backends) {
    var byQuery = latest[endpoint];
    received++;
    byQuery[norm(query)] = { query: query, backends: backends, seq: received };
    var keys = Object.keys(byQuery);
    if (keys.length > MAX_KEPT) {
      keys.sort(function(a, b) { return byQuery[a].seq - byQuery[b].seq; });
      delete byQuery[keys[0]];
    }
    scheduleRender();
  }

  // Graph view doesn't ask for stats; add the parameter so its range queries
  // report backends too. Responses are read from a clone, so Mantine sees them
  // unchanged.
  var origFetch = window.fetch;
  if (origFetch) {
    window.fetch = function(input, init) {
      var endpoint = null, query = null;
      try {
        if (typeof input === 'string') {
          var u = new URL(input, window.location.href);
          endpoint = endpointOf(u);
          if (endpoint) {
            query = u.searchParams.get('query');
            if (endpoint === 'query_range' && !u.searchParams.has('stats')) {
              u.searchParams.set('stats', 'true');
              input = u.toString();
            }
          }
        }
      } catch (e) {
        endpoint = null;
      }
      var p = origFetch.call(this, input, init);
      if (!endpoint || query === null) return p;
      return p.then(function(resp) {
        try {
          resp.clone().json().then(function(body) {
            var b = body && body.data && body.data.stats && body.data.stats.backends;
            if (b && b.groups) remember(endpoint, query, b);
          }, function() {});
        } catch (e) { /* not JSON: nothing to show */ }
        return resp;
      });
    };
  }

  /* ---------------------------------------------------------------- */
  /* Matching panels to stats                                          */
  /* ---------------------------------------------------------------- */

  function fiberOf(el) {
    for (var k in el) {
      if (k.indexOf('__reactFiber$') === 0) return el[k];
    }
    return null;
  }

  // The Table view component has props {panelIdx, expr, retriggerIdx}, the
  // Graph view one {expr, node, range, ...}; both hold the expression that was
  // last executed, which may differ from the editor's text.
  function panelQuery(panelEl) {
    var root = fiberOf(panelEl);
    if (!root) return null;
    var queue = [root], seen = 0;
    while (queue.length && seen < 400) {
      var f = queue.shift();
      seen++;
      var p = f.memoizedProps;
      if (p && typeof p === 'object' && typeof p.expr === 'string' && 'retriggerIdx' in p) {
        if ('panelIdx' in p) return { endpoint: 'query', expr: p.expr, node: null };
        if ('node' in p) return { endpoint: 'query_range', expr: p.expr, node: p.node };
      }
      for (var c = f.child; c; c = c.sibling) queue.push(c);
    }
    return null;
  }

  // An exact match on the executed expression; with an explain-tree node
  // selected, Graph queries only that subexpression, so fall back to the
  // newest stats whose query is part of the expression.
  function statsFor(pq) {
    var byQuery = latest[pq.endpoint];
    var exact = byQuery[norm(pq.expr)];
    if (exact && !pq.node) return exact;
    if (!pq.node) return null;
    var whole = norm(pq.expr), best = null;
    Object.keys(byQuery).forEach(function(k) {
      if (whole.indexOf(k) !== -1 && (!best || byQuery[k].seq > best.seq)) best = byQuery[k];
    });
    return best;
  }

  /* ---------------------------------------------------------------- */
  /* Rendering                                                         */
  /* ---------------------------------------------------------------- */

  function isOpen() {
    try { return window.localStorage.getItem(OPEN_KEY) === '1'; } catch (e) { return false; }
  }

  function setOpen(open) {
    try { window.localStorage.setItem(OPEN_KEY, open ? '1' : '0'); } catch (e) { /* not persisted */ }
  }

  function el(tag, style, text) {
    var e = document.createElement(tag);
    if (style) e.style.cssText = style;
    if (text !== undefined && text !== null) e.textContent = String(text);
    return e;
  }

  function fmtNum(n) {
    return Number(n || 0).toLocaleString('en-US');
  }

  function fmtDur(sec) {
    sec = Number(sec || 0);
    if (sec < 1) {
      var ms = sec * 1000;
      return (ms < 10 ? ms.toFixed(1) : Math.round(ms)) + ' ms';
    }
    return sec.toFixed(2) + ' s';
  }

  function fmtSpan(sec) {
    sec = Math.round(Number(sec || 0));
    if (sec >= 3600 && sec % 3600 === 0) return (sec / 3600) + 'h';
    if (sec >= 60 && sec % 60 === 0) return (sec / 60) + 'm';
    return sec + 's';
  }

  function groupLabel(g) {
    if (g.name && !/^sg-\d+$/.test(g.name)) return g.name;
    var labels = g.labels || {};
    var parts = Object.keys(labels).sort().map(function(k) { return k + '="' + labels[k] + '"'; });
    return parts.length ? parts.join(',') : (g.name || '#' + g.ordinal);
  }

  var MUTED = 'color:var(--mantine-color-dimmed);';
  var ERR = 'color:var(--mantine-color-red-text, #fa5252);';
  var CELL = 'padding:3px 10px 3px 0;border-bottom:1px solid var(--mantine-color-default-border);vertical-align:top;white-space:nowrap;';
  var NUM = CELL + 'text-align:right;font-variant-numeric:tabular-nums;';

  function buildBlock(entry) {
    var b = entry.backends;
    var groups = b.groups || [];
    var requests = b.requests || [];
    var open = isOpen();

    var block = el('div', 'font-size:var(--mantine-font-size-xs);margin:2px 0 6px;align-self:stretch;width:100%;');
    block.setAttribute(BLOCK_ATTR, String(entry.seq));

    var summary = el('div', 'cursor:pointer;user-select:none;display:flex;flex-wrap:wrap;gap:4px 14px;align-items:baseline;' + MUTED);
    summary.setAttribute('role', 'button');
    summary.setAttribute('aria-expanded', open ? 'true' : 'false');
    summary.appendChild(el('span', 'font-weight:600;', (open ? '▾ ' : '▸ ') + 'Backends'));
    groups.forEach(function(g) {
      var s = el('span', '', groupLabel(g) + ': ' + fmtNum(g.requests) + ' req · ' + fmtNum(g.series) +
        ' series · ' + fmtNum(g.samples) + ' samples · ' + fmtDur(g.duration));
      s.setAttribute('data-proxeus-group', g.name);
      if (g.errors) s.appendChild(el('span', ERR, ' · ' + fmtNum(g.errors) + ' failed'));
      summary.appendChild(s);
    });
    summary.addEventListener('click', function() {
      setOpen(!isOpen());
      rerenderAll();
    });
    block.appendChild(summary);

    if (open) {
      var wrap = el('div', 'overflow-x:auto;margin-top:4px;');
      var table = el('table', 'border-collapse:collapse;width:100%;');
      var head = el('tr');
      ['Group', 'Path', 'Query', 'Range', 'Time', 'Series', 'Samples', 'Status'].forEach(function(h, i) {
        head.appendChild(el('th', (i >= 4 && i <= 6 ? NUM : CELL) + 'font-weight:600;text-align:' + (i >= 4 && i <= 6 ? 'right' : 'left') + ';', h));
      });
      var thead = el('thead');
      thead.appendChild(head);
      table.appendChild(thead);
      var tbody = el('tbody');
      var labelByName = {};
      groups.forEach(function(g) { labelByName[g.name] = groupLabel(g); });
      requests.forEach(function(r) {
        var tr = el('tr');
        tr.appendChild(el('td', CELL, labelByName[r.group] || r.group));
        tr.appendChild(el('td', CELL, (r.path || '-') + ' / ' + r.op));
        tr.appendChild(el('td', CELL + 'font-family:var(--mantine-font-family-monospace);white-space:normal;word-break:break-all;', r.query));
        var range = r.start === r.end ? 'instant' : fmtSpan(r.end - r.start);
        if (r.step) range += ', step ' + fmtSpan(r.step);
        tr.appendChild(el('td', CELL + MUTED, range));
        tr.appendChild(el('td', NUM, fmtDur(r.duration)));
        tr.appendChild(el('td', NUM, fmtNum(r.series)));
        tr.appendChild(el('td', NUM, fmtNum(r.samples)));
        tr.appendChild(el('td', CELL + (r.status === 'error' ? ERR + 'white-space:normal;' : ''), r.status === 'error' ? 'error: ' + (r.error || '') : 'ok'));
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      wrap.appendChild(table);
      block.appendChild(wrap);
      if (b.requestsTruncated) {
        block.appendChild(el('div', MUTED + 'margin-top:2px;', fmtNum(b.requestsTruncated) + ' more requests not listed (counted in the totals).'));
      }
    }
    return block;
  }

  // The stats sit under the panel's first row(s) of controls: the
  // "Load time" row in Table view, the time and display rows in Graph view.
  function anchorIn(panelEl) {
    var stack = panelEl.firstElementChild;
    if (!stack) return null;
    var anchor = null;
    for (var c = stack.firstElementChild; c; c = c.nextElementSibling) {
      if (c.hasAttribute(BLOCK_ATTR)) continue;
      if (!c.classList.contains('mantine-Group-root')) break;
      anchor = c;
    }
    return anchor;
  }

  var forceAll = false;

  function renderPanels() {
    var panels = document.querySelectorAll('.mantine-Tabs-panel');
    for (var i = 0; i < panels.length; i++) {
      var panel = panels[i];
      if (panel.offsetParent === null) continue;
      var existing = panel.querySelector('[' + BLOCK_ATTR + ']');
      var pq = panelQuery(panel);
      var entry = pq ? statsFor(pq) : null;
      if (!entry) {
        if (existing) existing.remove();
        continue;
      }
      if (existing && !forceAll && existing.getAttribute(BLOCK_ATTR) === String(entry.seq)) continue;
      var anchor = anchorIn(panel);
      if (!anchor) continue;
      if (existing) existing.remove();
      anchor.insertAdjacentElement('afterend', buildBlock(entry));
    }
    forceAll = false;
  }

  function rerenderAll() {
    forceAll = true;
    renderPanels();
  }

  var pending = false;
  function scheduleRender() {
    if (pending) return;
    pending = true;
    window.requestAnimationFrame(function() {
      pending = false;
      try { renderPanels(); } catch (e) { /* never break the page */ }
    });
  }

  function start() {
    new MutationObserver(scheduleRender).observe(document.body, { childList: true, subtree: true });
    scheduleRender();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
