(function(){
  var PREFIX = (window.__PROXEUS_ROUTE_PREFIX__ || '');
  if (window.__proxeusPage !== PREFIX + '/cardinality') return;
  var API_URL = PREFIX + '/proxeus/api/cardinality.json';

  // Mantine's theme variables, as on the Backends page (nav.js).
  var TEXT = 'color:var(--mantine-color-text);';
  var MUTED = 'color:var(--mantine-color-dimmed);';
  var ERR = 'color:var(--mantine-color-red-text, #fa5252);';
  var BORDER = '1px solid var(--mantine-color-default-border)';
  var TINT = 'background:var(--mantine-color-gray-light);';
  var MONO = 'font-family:var(--mantine-font-family-monospace);';
  var CELL = 'padding:6px 12px;text-align:left;vertical-align:top;border-bottom:' + BORDER + ';overflow-wrap:anywhere;';
  var INPUT = 'padding:6px 8px;border:' + BORDER + ';border-radius:4px;background:var(--mantine-color-body);' + TEXT + 'font-size:0.85rem;';

  // [field, title, name column, value column]
  var LISTS = [
    ['seriesCountByMetricName', 'Series by metric name', 'Metric', 'Series'],
    ['seriesCountByLabelValuePair', 'Series by label=value pair', 'Pair', 'Series'],
    ['labelValueCountByLabelName', 'Values per label name', 'Label', 'Values'],
    ['seriesCountByLabelName', 'Series by label name', 'Label', 'Series'],
    ['seriesCountByFocusLabelValue', 'Series by focus label value', 'Value', 'Series'],
    ['memoryInBytesByLabelName', 'Memory by label name', 'Label', 'Bytes']
  ];
  var FILTER_NAMES = { 'match[]': 'series filter', 'focusLabel': 'focus label', 'date': 'date' };

  var params = new URLSearchParams(window.location.search);
  var filters = {
    topN: params.get('topN') || '10',
    match: params.get('match[]') || '',
    focusLabel: params.get('focusLabel') || '',
    date: params.get('date') || ''
  };

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function num(n) { return Number(n || 0).toLocaleString(); }

  function bytes(n) {
    var units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
    var i = 0;
    n = Number(n || 0);
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return (i === 0 ? n : n.toFixed(1)) + ' ' + units[i];
  }

  function time(ms) { return new Date(ms).toLocaleString(); }

  function badgeColor(btype) {
    var map = {
      'thanos': '#4dabf7',
      'victoriametrics': '#51cf66',
      'cortex': '#f783ac',
      'mimir': '#cc5de8',
      'prometheus': '#e67700'
    };
    return map[(btype || '').toLowerCase()] || '#868e96';
  }

  function field(id, label, value, attrs, style) {
    return '<label style="display:flex;flex-direction:column;gap:4px;font-size:0.75rem;' + MUTED + '">' + label +
      '<input id="' + id + '" value="' + escHtml(value) + '" ' + attrs + ' style="' + INPUT + (style || '') + '"></label>';
  }

  function skeleton() {
    return '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">' +
      '<h2 style="margin:0;font-size:1.25rem;font-weight:600;' + TEXT + '">Cardinality</h2>' +
      '<span id="proxeus-cardinality-status" style="font-size:0.75rem;' + MUTED + '"></span>' +
      '</div>' +
      '<form id="proxeus-cardinality-form" style="display:flex;flex-wrap:wrap;align-items:flex-end;gap:12px;margin-bottom:8px;">' +
      field('proxeus-cardinality-topn', 'Top N', filters.topN, 'type="number" min="1" max="1000"', 'width:90px;') +
      field('proxeus-cardinality-match', 'Series filter (match[])', filters.match, 'type="text" placeholder="{job=&quot;node&quot;}" spellcheck="false"') +
      field('proxeus-cardinality-focus', 'Focus label', filters.focusLabel, 'type="text" placeholder="instance" spellcheck="false"') +
      field('proxeus-cardinality-date', 'Date (UTC)', filters.date, 'type="date"') +
      '<button id="proxeus-cardinality-apply" type="submit" style="padding:7px 16px;border:none;border-radius:4px;' +
      'background:var(--mantine-color-blue-filled, #228be6);color:#fff;font-size:0.85rem;cursor:pointer;">Apply</button>' +
      '</form>' +
      '<p style="margin:0 0 16px;font-size:0.8rem;' + MUTED + '">Each backend is shown separately: series kept by more than one ' +
      'backend would be counted twice in a sum. Series filter, focus label and date apply to VictoriaMetrics only.</p>' +
      '<div id="proxeus-cardinality-groups"></div>';
  }

  function coverageText(g) {
    var c = g.coverage;
    if (!c) return '';
    if (c.kind === 'head') return 'Head block: ' + time(c.minTime) + ' to ' + time(c.maxTime);
    var s = 'Index for ' + c.date + ' (UTC)';
    if ((g.backendType || '') === 'victoriametrics') s += '; a cluster sums its storage nodes, so counts can be upper bounds';
    return s;
  }

  function listTable(g, list) {
    var rows = g[list[0]];
    if (!rows || rows.length === 0) return '';
    var fmt = list[0] === 'memoryInBytesByLabelName' ? bytes : num;
    var html = '<table data-list="' + list[0] + '" style="width:100%;table-layout:fixed;border-collapse:collapse;font-size:0.82rem;">' +
      '<colgroup><col style="width:72%"><col style="width:28%"></colgroup>' +
      '<caption style="text-align:left;padding:0 0 6px;font-size:0.85rem;font-weight:600;' + TEXT + '">' + escHtml(list[1]) + '</caption>' +
      '<thead><tr style="' + TINT + '">' +
      '<th style="' + CELL + TEXT + 'font-weight:600;">' + list[2] + '</th>' +
      '<th style="' + CELL + TEXT + 'font-weight:600;text-align:right;">' + list[3] + '</th>' +
      '</tr></thead><tbody>';
    rows.forEach(function(r) {
      html += '<tr><td style="' + CELL + TEXT + MONO + '">' + escHtml(r.name) + '</td>' +
        '<td style="' + CELL + TEXT + 'text-align:right;white-space:nowrap;">' + escHtml(fmt(r.value)) + '</td></tr>';
    });
    return html + '</tbody></table>';
  }

  function card(g) {
    var html = '<div data-group="' + escHtml(g.name) + '" style="background:var(--mantine-color-body);border:' + BORDER +
      ';border-radius:8px;margin-bottom:16px;overflow:hidden;">';
    html += '<div style="padding:12px 16px;' + TINT + 'border-bottom:' + BORDER + ';display:flex;flex-wrap:wrap;align-items:center;gap:8px;">' +
      '<strong data-group-name style="font-size:0.9rem;' + TEXT + '">' + escHtml(g.name) + '</strong>' +
      '<span style="display:inline-block;padding:2px 8px;border-radius:4px;background:' + badgeColor(g.backendType) +
      ';color:#fff;font-size:0.75rem;font-weight:600;">' + escHtml(g.backendType || '-') + '</span>' +
      (g.target ? '<span style="font-size:0.8rem;' + MUTED + MONO + 'overflow-wrap:anywhere;">' + escHtml(g.target) + '</span>' : '') +
      '</div>';
    html += '<div style="padding:12px 16px;display:flex;flex-direction:column;gap:8px;">';
    if (g.error) {
      html += '<p data-error style="margin:0;font-size:0.85rem;' + ERR + 'overflow-wrap:anywhere;">' + escHtml(g.error) + '</p>';
    } else {
      html += '<div style="font-size:0.85rem;' + TEXT + '">Series: <strong data-total-series>' + escHtml(num(g.totalSeries)) +
        '</strong> · Label=value pairs: <strong>' + escHtml(num(g.totalLabelValuePairs)) + '</strong></div>';
      html += '<div data-coverage style="font-size:0.8rem;' + TEXT + '">' + escHtml(coverageText(g)) + '</div>';
      if (g.partial) {
        html += '<div style="font-size:0.8rem;' + ERR + '">Partial response: some storage nodes did not answer.</div>';
      }
    }
    if (g.notApplied && g.notApplied.length) {
      html += '<div data-not-applied style="font-size:0.8rem;' + TEXT + '">Not applied, this backend does not support it: ' +
        escHtml(g.notApplied.map(function(f) { return FILTER_NAMES[f] || f; }).join(', ')) + '</div>';
    }
    if (!g.error) {
      var tables = LISTS.map(function(l) { return listTable(g, l); }).filter(Boolean);
      if (tables.length) {
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(min(100%,420px),1fr));gap:16px;margin-top:4px;">' +
          tables.map(function(t) { return '<div>' + t + '</div>'; }).join('') + '</div>';
      } else {
        html += '<p style="margin:0;font-size:0.85rem;' + MUTED + '">No series.</p>';
      }
    }
    return html + '</div></div>';
  }

  function load() {
    var statusEl = document.getElementById('proxeus-cardinality-status');
    var groupsEl = document.getElementById('proxeus-cardinality-groups');
    statusEl.textContent = 'Loading...';
    var q = new URLSearchParams();
    q.set('topN', filters.topN || '10');
    if (filters.match) q.set('match[]', filters.match);
    if (filters.focusLabel) q.set('focusLabel', filters.focusLabel);
    if (filters.date) q.set('date', filters.date);

    fetch(API_URL + '?' + q.toString())
      .then(function(r) {
        if (!r.ok) return r.text().then(function(t) { throw new Error(t.trim() || ('HTTP ' + r.status)); });
        return r.json();
      })
      .then(function(data) {
        var groups = data.groups || [];
        groupsEl.innerHTML = groups.length ? groups.map(card).join('') :
          '<p style="' + MUTED + '">No backend groups configured.</p>';
        statusEl.textContent = 'Updated ' + new Date().toLocaleTimeString();
      })
      .catch(function(err) {
        groupsEl.innerHTML = '<p style="' + ERR + '">Failed to load cardinality: ' + escHtml(err.message || err) + '</p>';
        statusEl.textContent = '';
      });
  }

  var rendered = false;
  function render() {
    if (rendered) return;
    var main = document.querySelector('main') || document.querySelector('[class*="_main_"]');
    if (!main) return;
    rendered = true;
    var container = document.createElement('div');
    container.id = 'proxeus-cardinality-root';
    container.style.cssText = 'padding:16px;font-family:system-ui,-apple-system,sans-serif;';
    container.innerHTML = skeleton();
    main.innerHTML = '';
    main.appendChild(container);
    // Restore normal navigation now that we own the page (see head.js).
    window.__proxeusNavReady = true;

    document.getElementById('proxeus-cardinality-form').addEventListener('submit', function(e) {
      e.preventDefault();
      filters.topN = document.getElementById('proxeus-cardinality-topn').value.trim();
      filters.match = document.getElementById('proxeus-cardinality-match').value.trim();
      filters.focusLabel = document.getElementById('proxeus-cardinality-focus').value.trim();
      filters.date = document.getElementById('proxeus-cardinality-date').value.trim();
      load();
    });
    load();
  }

  var observer = new MutationObserver(function() {
    render();
    if (rendered) observer.disconnect();
  });
  observer.observe(document.body, { childList: true, subtree: true });
  render();
})();
