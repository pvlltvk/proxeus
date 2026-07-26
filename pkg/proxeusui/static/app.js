/* Proxeus backend inventory — polling client */
/* No frameworks, no build step. Vanilla JS only. */

'use strict';

(function () {
  var POLL_INTERVAL_MS = 5000;
  var BACKENDS_URL     = '/proxeus/api/backends.json';

  /* ------------------------------------------------------------------ */
  /* DOM references — populated on DOMContentLoaded                       */
  /* ------------------------------------------------------------------ */
  var elRefreshDot;
  var elRefreshText;
  var elErrorBanner;
  var elGeneratedAt;
  var elGroupsContainer;

  /* ------------------------------------------------------------------ */
  /* Utilities                                                            */
  /* ------------------------------------------------------------------ */

  /**
   * Return a human-readable relative time string for an ISO-8601 timestamp.
   * E.g. "just now", "12s ago", "3m ago", "2h ago".
   */
  function relativeTime(isoString) {
    if (!isoString) return null;
    var t = new Date(isoString);
    if (isNaN(t.getTime())) return null;
    var diffMs  = Date.now() - t.getTime();
    var diffSec = Math.floor(diffMs / 1000);
    if (diffSec < 5)   return 'just now';
    if (diffSec < 60)  return diffSec + 's ago';
    var diffMin = Math.floor(diffSec / 60);
    if (diffMin < 60)  return diffMin + 'm ago';
    var diffHr  = Math.floor(diffMin / 60);
    if (diffHr  < 24)  return diffHr + 'h ago';
    var diffDay = Math.floor(diffHr / 24);
    return diffDay + 'd ago';
  }

  /**
   * Format an ISO-8601 string as "YYYY-MM-DD HH:MM:SS UTC" for display.
   */
  function formatAbsolute(isoString) {
    if (!isoString) return 'unknown';
    var t = new Date(isoString);
    if (isNaN(t.getTime())) return isoString;
    var pad = function (n) { return String(n).padStart(2, '0'); };
    return t.getUTCFullYear() + '-'
      + pad(t.getUTCMonth() + 1) + '-'
      + pad(t.getUTCDate())      + ' '
      + pad(t.getUTCHours())     + ':'
      + pad(t.getUTCMinutes())   + ':'
      + pad(t.getUTCSeconds())   + ' UTC';
  }

  /** Return the CSS modifier class for a given health state. */
  function healthClass(healthy, lastProbeAt) {
    if (!lastProbeAt) return 'unknown';
    return healthy ? 'healthy' : 'unhealthy';
  }

  /** Return a display label for the health state. */
  function healthLabel(healthy, lastProbeAt) {
    if (!lastProbeAt) return 'Unknown';
    return healthy ? 'Up' : 'Down';
  }

  /** Truncate a string to maxLen, appending ellipsis if needed. */
  function truncate(str, maxLen) {
    if (!str || str.length <= maxLen) return str || '';
    return str.slice(0, maxLen) + '…';
  }

  /** Escape a string for safe insertion into HTML text nodes via innerHTML. */
  function esc(str) {
    if (str === null || str === undefined) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  /* ------------------------------------------------------------------ */
  /* HTML fragment builders                                               */
  /* ------------------------------------------------------------------ */

  function buildBadge(backendType) {
    var type  = (backendType || 'unknown').toLowerCase();
    var label = backendType || 'unknown';
    /* Abbreviate "victoriametrics" to "VictoriaMetrics" for compactness */
    if (type === 'victoriametrics') label = 'VictoriaMetrics';
    else if (type === 'thanos')     label = 'Thanos';
    else if (type === 'prometheus') label = 'Prometheus';
    return '<span class="backend-badge backend-badge--' + esc(type) + '">' + esc(label) + '</span>';
  }

  function buildLabelChips(labels) {
    if (!labels || typeof labels !== 'object') return '';
    var keys = Object.keys(labels).sort();
    return keys.map(function (k) {
      return '<span class="label-chip">' + esc(k) + '=' + esc(labels[k]) + '</span>';
    }).join('');
  }

  function buildTargetRow(target) {
    var hClass = healthClass(target.healthy, target.lastProbeAt);
    var hLabel = healthLabel(target.healthy, target.lastProbeAt);

    /* Health cell */
    var healthCell = '<span class="health">'
      + '<span class="health-dot health-dot--' + hClass + '"></span>'
      + '<span class="health-text--' + hClass + '">' + esc(hLabel) + '</span>'
      + '</span>';

    /* URL cell */
    var urlCell = '<a class="target-url" href="' + esc(target.url) + '" target="_blank" rel="noopener">'
      + esc(target.url) + '</a>';

    /* Backend + version cell */
    var versionStr = target.version ? (' <span class="version-str">' + esc(target.version) + '</span>') : '';
    var backendCell = buildBadge(target.backendType) + versionStr;

    /* Last probed cell */
    var relTs   = relativeTime(target.lastProbeAt);
    var absTs   = formatAbsolute(target.lastProbeAt);
    var probeCell;
    if (!relTs) {
      probeCell = '<span class="ts-never">Never</span>';
    } else {
      probeCell = '<span class="ts-relative" title="' + esc(absTs) + '">' + esc(relTs) + '</span>';
    }

    /* Error cell (only shown when unhealthy) */
    var errorCell = '';
    if (!target.healthy && target.lastError) {
      var short = truncate(target.lastError, 120);
      errorCell = '<td><span class="target-error" title="' + esc(target.lastError) + '">'
        + esc(short) + '</span></td>';
    } else {
      errorCell = '<td></td>';
    }

    return '<tr>'
      + '<td>' + healthCell + '</td>'
      + '<td>' + urlCell    + '</td>'
      + '<td>' + backendCell + '</td>'
      + '<td>' + probeCell  + '</td>'
      + errorCell
      + '</tr>';
  }

  function buildGroupCard(group) {
    var targets  = group.targets || [];
    var upCount  = targets.filter(function (t) { return t.healthy; }).length;
    var summary  = upCount + ' / ' + targets.length + ' up';

    /* Group header */
    var html = '<div class="group-card" data-group="' + esc(group.name) + '">'
      + '<div class="group-header">'
      + '<span class="group-header__ordinal" title="server_group YAML index (ordinal ' + esc(String(group.ordinal)) + ')">ord:' + esc(String(group.ordinal)) + '</span>'
      + '<span class="group-header__name">' + esc(group.name) + '</span>'
      + '<span class="group-header__labels">' + buildLabelChips(group.labels) + '</span>'
      + '<span class="group-header__summary">' + esc(summary) + '</span>'
      + '</div>';

    /* Targets table */
    if (targets.length === 0) {
      html += '<div class="empty-state">No targets configured for this group.</div>';
    } else {
      html += '<table class="targets-table">'
        + '<thead><tr>'
        + '<th>Health</th>'
        + '<th>Endpoint</th>'
        + '<th>Backend</th>'
        + '<th>Last probed</th>'
        + '<th>Last error</th>'
        + '</tr></thead>'
        + '<tbody>'
        + targets.map(buildTargetRow).join('')
        + '</tbody>'
        + '</table>';
    }

    html += '</div>';
    return html;
  }

  function buildInventory(data) {
    var groups = data.groups || [];
    if (groups.length === 0) {
      return '<div class="empty-state">No server groups configured.</div>';
    }
    return groups.map(buildGroupCard).join('');
  }

  /* ------------------------------------------------------------------ */
  /* DOM update helpers                                                   */
  /* ------------------------------------------------------------------ */

  function setGeneratedAt(isoString) {
    if (!elGeneratedAt) return;
    elGeneratedAt.textContent = 'Last refreshed: ' + formatAbsolute(isoString);
    elGeneratedAt.setAttribute('title', isoString || '');
  }

  function setRefreshing(active) {
    if (!elRefreshDot) return;
    if (active) {
      elRefreshDot.classList.add('refresh-dot--active');
    } else {
      elRefreshDot.classList.remove('refresh-dot--active');
    }
  }

  function showError(msg) {
    if (!elErrorBanner) return;
    elErrorBanner.textContent = msg;
    elErrorBanner.classList.add('error-banner--visible');
  }

  function clearError() {
    if (!elErrorBanner) return;
    elErrorBanner.textContent = '';
    elErrorBanner.classList.remove('error-banner--visible');
  }

  /* ------------------------------------------------------------------ */
  /* Fetch + render cycle                                                 */
  /* ------------------------------------------------------------------ */

  function fetchAndRender() {
    setRefreshing(true);

    fetch(BACKENDS_URL, { cache: 'no-store' })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error('HTTP ' + resp.status + ' ' + resp.statusText);
        }
        return resp.json();
      })
      .then(function (data) {
        clearError();

        /* Rebuild inventory in a single innerHTML assignment to avoid flicker */
        if (elGroupsContainer) {
          elGroupsContainer.innerHTML = buildInventory(data);
        }

        setGeneratedAt(data.generatedAt);
      })
      .catch(function (err) {
        var now = new Date();
        var hh  = String(now.getHours()).padStart(2, '0');
        var mm  = String(now.getMinutes()).padStart(2, '0');
        var ss  = String(now.getSeconds()).padStart(2, '0');
        showError('Last refresh failed at ' + hh + ':' + mm + ':' + ss + ' — retrying… (' + err.message + ')');
      })
      .finally(function () {
        setRefreshing(false);
      });
  }

  /* ------------------------------------------------------------------ */
  /* Bootstrap                                                            */
  /* ------------------------------------------------------------------ */

  document.addEventListener('DOMContentLoaded', function () {
    elRefreshDot       = document.getElementById('js-refresh-dot');
    elRefreshText      = document.getElementById('js-refresh-text');
    elErrorBanner      = document.getElementById('js-error-banner');
    elGeneratedAt      = document.getElementById('js-generated-at');
    elGroupsContainer  = document.getElementById('js-groups-container');

    /* Initial fetch immediately, then poll */
    fetchAndRender();
    setInterval(fetchAndRender, POLL_INTERVAL_MS);

    /* Keep relative times fresh every 15 seconds without a full fetch */
    setInterval(function () {
      var cells = document.querySelectorAll('.ts-relative');
      cells.forEach(function (el) {
        var isoStr = el.getAttribute('title');
        if (!isoStr) return;
        var rel = relativeTime(isoStr);
        if (rel) el.textContent = rel;
      });
    }, 15000);
  });
}());
