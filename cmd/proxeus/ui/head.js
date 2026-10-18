(function(){
  var PREFIX = (window.__PROXEUS_ROUTE_PREFIX__ || '');
  // Proxeus's own pages, rendered into the Mantine shell by nav.js and
  // cardinality.js.
  var PAGES = ['/backends', '/cardinality'];
  var path = window.location.pathname.replace(/\/$/, '');
  var bp = null;
  for (var i = 0; i < PAGES.length; i++) {
    if (path === PREFIX + PAGES[i]) bp = PREFIX + PAGES[i];
  }
  if (!bp) return;
  window.__proxeusPage = bp;
  var origPush = history.pushState;
  var origReplace = history.replaceState;
  history.pushState = function(s,t,u) { if (!window.__proxeusNavReady) return; return origPush.apply(this,arguments); };
  history.replaceState = function(s,t,u) { if (!window.__proxeusNavReady) return; return origReplace.apply(this,arguments); };
  // The UI takes its path prefix from the page URL and doesn't know these
  // pages, so here it would call <prefix>/backends/api/... (the
  // notifications stream, /-/ready). Send those to the real prefix.
  var origFetch = window.fetch;
  window.fetch = function(input, init) {
    if (typeof input === 'string' && input.indexOf(bp + '/') === 0) input = PREFIX + input.slice(bp.length);
    return origFetch.call(this, input, init);
  };
})();
