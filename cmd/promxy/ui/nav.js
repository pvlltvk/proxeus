(function(){
  var ICON_URL = '/promxy/static/promxy-icon.svg';
  var BACKENDS_PATH = '/backends';
  var API_URL = '/promxy/api/backends.json';

  // Text labels for nav items to hide from the dropdown.
  var HIDE_DROPDOWN_LABELS = {
    'Target health': true,
    'Rule health': true,
    'Service discovery': true,
    'TSDB status': true,
    'Alertmanager discovery': true
  };

  // Top-level nav links to hide — matched by link text so relative URLs
  // (e.g. /backends/alerts) don't break the match.
  var HIDE_NAV_TEXTS = {
    'Alerts': true,
    'Targets': true,
    'Rules': true,
    'Service discovery': true,
    'TSDB status': true,
    'Alertmanager discovery': true
  };

  // --- Href fixer for the backends page ---
  // React Router generates relative hrefs resolved against /backends and
  // also intercepts clicks with its own onClick handler that calls navigate()
  // relatively, producing /backends/query instead of /query.
  // We fix both problems: rewrite href AND override the click to do a full
  // browser navigation (bypassing React Router entirely).
  function fixBackendsHref(a) {
    // a.href is the fully-resolved absolute URL; use pathname to compare.
    var pathname = a.pathname; // e.g. /backends/query
    var prefix = BACKENDS_PATH + '/';
    if (pathname && pathname.indexOf(prefix) === 0) {
      var corrected = '/' + pathname.slice(prefix.length);
      a.href = corrected;
      // Mark so we only attach the listener once.
      if (!a.dataset.promxyFixed) {
        a.dataset.promxyFixed = '1';
        a.addEventListener('click', function(e) {
          e.preventDefault();
          e.stopImmediatePropagation();
          window.location.href = a.href;
        }, true); // capture phase — fires before React's bubble handler
      }
    }
  }

  // --- One-time header customisation ---
  var headerDone = false;
  function customiseHeader() {
    if (headerDone) return false;
    var header = document.querySelector('header');
    if (!header) return false;
    var links = header.querySelectorAll('a');
    if (links.length === 0) return false;

    // Logo swap.
    var img = header.querySelector('img');
    if (img) img.src = ICON_URL;

    // Title text.
    var walker = document.createTreeWalker(header, NodeFilter.SHOW_TEXT, null, false);
    var node;
    while ((node = walker.nextNode())) {
      if (node.nodeValue.indexOf('Prometheus') !== -1) {
        node.nodeValue = node.nodeValue.replace(/Prometheus/g, 'Promxy');
      }
    }

    // Hide top-level nav links by text content (header + side nav).
    document.querySelectorAll('header a, nav a').forEach(function(a) {
      var text = (a.textContent || '').trim();
      if (HIDE_NAV_TEXTS[text]) {
        a.style.display = 'none';
      }
    });

    // On the backends page, React Router generates relative hrefs that resolve
    // against /backends, e.g. "query" -> /backends/query.
    // Rewrite those to absolute paths so navigation works correctly.
    if (window.__promxyBackends) {
      document.querySelectorAll('header a, nav a').forEach(function(a) {
        fixBackendsHref(a);
      });
    }

    headerDone = true;
    return true;
  }

  // --- Dropdown customisation (runs every time the popover opens) ---
  function customiseDropdown() {
    var menus = document.querySelectorAll('[data-menu-dropdown]');
    menus.forEach(function(menu) {
      if (menu.dataset.promxyProcessed) return;

      // In Mantine, menu items are <a data-menu-item> directly (not wrapped).
      // Labels use class .mantine-Menu-label, dividers use .mantine-Menu-divider.
      var items = menu.querySelectorAll('[data-menu-item]');
      var monitoringLabel = null;
      var serverStatusLabel = null;

      menu.querySelectorAll('.mantine-Menu-label').forEach(function(lbl) {
        var txt = (lbl.textContent || '').trim();
        if (txt === 'Monitoring status') monitoringLabel = lbl;
        if (txt === 'Server status') serverStatusLabel = lbl;
      });

      // Hide unwanted items — the <a> itself has data-menu-item and textContent.
      // Save "Target health" before hiding it so we can reuse its icon.
      var iconDonorItem = null;
      items.forEach(function(item) {
        var text = (item.textContent || '').trim();
        if (text === 'Target health') iconDonorItem = item;
        if (HIDE_DROPDOWN_LABELS[text]) {
          item.style.display = 'none';
        }
      });

      // Hide "Monitoring status" label and divider below it.
      if (monitoringLabel) {
        monitoringLabel.style.display = 'none';
        var sib = monitoringLabel.nextElementSibling;
        while (sib && sib !== serverStatusLabel) {
          if (sib.classList && sib.classList.contains('mantine-Menu-divider')) {
            sib.style.display = 'none';
          }
          sib = sib.nextElementSibling;
        }
      }

      // Inject "Backends" as the first item under "Server status".
      if (!menu.querySelector('[data-promxy-backends]') && serverStatusLabel) {
        // Prefer cloning a hidden item (distinct icon); fall back to first visible item.
        var templateItem = iconDonorItem;
        if (!templateItem) {
          items.forEach(function(item) {
            if (!templateItem && item.style.display !== 'none') templateItem = item;
          });
        }

        if (templateItem) {
          // Deep-clone so we get the icon SVG and all child spans.
          var backendsLink = templateItem.cloneNode(true);
          backendsLink.style.display = '';
          backendsLink.href = BACKENDS_PATH;
          backendsLink.setAttribute('data-promxy-backends', '');
          // Update only the visible text node(s), leaving the icon intact.
          // Mantine items typically have: <svg>…</svg><span>Label text</span>
          var textSpan = backendsLink.querySelector('span:last-of-type') ||
                         backendsLink.querySelector('span');
          if (textSpan) {
            textSpan.textContent = 'Backends';
          } else {
            // Fallback: replace full text content preserving icon nodes.
            var tw = document.createTreeWalker(backendsLink, NodeFilter.SHOW_TEXT, null, false);
            var tn;
            while ((tn = tw.nextNode())) {
              if (tn.nodeValue && tn.nodeValue.trim().length > 0) {
                tn.nodeValue = 'Backends';
                break;
              }
            }
          }
          serverStatusLabel.insertAdjacentElement('afterend', backendsLink);
        }
      }

      // On the backends page, fix any relative hrefs in the dropdown.
      if (window.__promxyBackends) {
        menu.querySelectorAll('a').forEach(function(a) {
          fixBackendsHref(a);
        });
      }

      menu.dataset.promxyProcessed = '1';
    });
  }

  // --- Backends page renderer ---
  var isBackendsPage = !!window.__promxyBackends;
  var backendsRendered = false;
  var backendsInterval = null;

  function renderBackendsPage() {
    if (!isBackendsPage) return;

    var main = document.querySelector('main') || document.querySelector('[class*="_main_"]');
    if (!main) return;

    var container = document.getElementById('promxy-backends-root');
    if (!container) {
      container = document.createElement('div');
      container.id = 'promxy-backends-root';
      container.style.cssText = 'padding:16px;font-family:system-ui,-apple-system,sans-serif;';
      main.innerHTML = '';
      main.appendChild(container);
      backendsRendered = true;
      // Restore normal navigation now that we own the page.
      window.__promxyNavReady = true;
    }

    if (!backendsInterval) {
      loadBackendsData(container);
      backendsInterval = setInterval(function() { loadBackendsData(container); }, 5000);
    }
  }

  function relTime(isoStr) {
    if (!isoStr) return '-';
    var diff = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
    if (diff < 60) return diff + 's ago';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    return Math.floor(diff / 3600) + 'h ago';
  }

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

  function loadBackendsData(container) {
    var statusEl = document.getElementById('promxy-backends-status');
    if (statusEl) statusEl.textContent = 'Refreshing...';

    fetch(API_URL)
      .then(function(r) { return r.json(); })
      .then(function(data) { renderBackendsData(container, data); })
      .catch(function(err) {
        container.innerHTML = '<p style="color:#fa5252">Failed to load backends: ' + err + '</p>';
      });
  }

  function renderBackendsData(container, data) {
    var groups = data.groups || [];
    var now = new Date().toLocaleTimeString();
    var html = '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">' +
      '<h2 style="margin:0;font-size:1.25rem;font-weight:600;color:#1a1a1a;">Backends</h2>' +
      '<span id="promxy-backends-status" style="font-size:0.75rem;color:#868e96;">Updated ' + now + '</span>' +
      '</div>';

    if (groups.length === 0) {
      html += '<p style="color:#868e96;">No backend groups configured.</p>';
    } else {
      groups.forEach(function(group) {
        var targets = group.targets || [];
        var totalUp = targets.filter(function(t) { return t.healthy; }).length;
        var dotColor = targets.length === 0 ? '#aaa' : (totalUp === 0 ? '#fa5252' : totalUp < targets.length ? '#fab005' : '#40c057');

        html += '<div style="background:#fff;border:1px solid #e9ecef;border-radius:8px;margin-bottom:16px;overflow:hidden;">';
        html += '<div style="padding:12px 16px;background:#f8f9fa;border-bottom:1px solid #e9ecef;display:flex;align-items:center;gap:8px;">' +
          '<span style="width:10px;height:10px;border-radius:50%;background:' + dotColor + ';display:inline-block;flex-shrink:0;"></span>' +
          '<strong style="font-size:0.9rem;">' + escHtml(group.name) + '</strong>' +
          '<span style="color:#868e96;font-size:0.8rem;margin-left:auto;">' + totalUp + '/' + targets.length + ' up</span>' +
          '</div>';

        if (targets.length > 0) {
          html += '<table style="width:100%;border-collapse:collapse;font-size:0.85rem;">';
          html += '<thead><tr style="background:#f8f9fa;">' +
            '<th style="padding:8px 16px;text-align:left;color:#495057;font-weight:600;border-bottom:1px solid #e9ecef;">Endpoint</th>' +
            '<th style="padding:8px 16px;text-align:left;color:#495057;font-weight:600;border-bottom:1px solid #e9ecef;">Type</th>' +
            '<th style="padding:8px 16px;text-align:left;color:#495057;font-weight:600;border-bottom:1px solid #e9ecef;">Last Probed</th>' +
            '<th style="padding:8px 16px;text-align:left;color:#495057;font-weight:600;border-bottom:1px solid #e9ecef;">Last Error</th>' +
            '</tr></thead><tbody>';

          targets.forEach(function(t, i) {
            var rowBg = i % 2 === 0 ? '#fff' : '#fdfdfd';
            var tDot = t.healthy ? '#40c057' : '#fa5252';
            var bColor = badgeColor(t.backendType);
            html += '<tr style="background:' + rowBg + ';">';
            html += '<td style="padding:8px 16px;border-bottom:1px solid #f1f3f5;">' +
              '<span style="width:8px;height:8px;border-radius:50%;background:' + tDot + ';display:inline-block;margin-right:6px;flex-shrink:0;vertical-align:middle;"></span>' +
              '<a href="' + escHtml(t.url) + '" style="color:#228be6;text-decoration:none;font-family:monospace;font-size:0.82rem;" target="_blank" rel="noopener">' + escHtml(t.url) + '</a>' +
              '</td>';
            html += '<td style="padding:8px 16px;border-bottom:1px solid #f1f3f5;">' +
              '<span style="display:inline-block;padding:2px 8px;border-radius:4px;background:' + bColor + ';color:#fff;font-size:0.75rem;font-weight:600;">' + escHtml(t.backendType || '-') + '</span>' +
              '</td>';
            html += '<td style="padding:8px 16px;border-bottom:1px solid #f1f3f5;color:#495057;">' + escHtml(relTime(t.lastProbeAt)) + '</td>';
            html += '<td style="padding:8px 16px;border-bottom:1px solid #f1f3f5;color:' + (t.lastError ? '#fa5252' : '#868e96') + ';font-size:0.8rem;">' + escHtml(t.lastError || '-') + '</td>';
            html += '</tr>';
          });

          html += '</tbody></table>';
        } else {
          html += '<p style="padding:12px 16px;color:#868e96;margin:0;">No targets in this group.</p>';
        }
        html += '</div>';
      });
    }

    container.innerHTML = html;
  }

  function escHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  // --- MutationObserver: stays connected for the page lifetime ---
  var observer = new MutationObserver(function() {
    customiseHeader();
    customiseDropdown();
    if (!backendsRendered && window.location.pathname === BACKENDS_PATH) {
      renderBackendsPage();
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });

  // Also run immediately in case React already rendered.
  customiseHeader();
  customiseDropdown();
  if (window.location.pathname === BACKENDS_PATH) {
    renderBackendsPage();
  }
})();
