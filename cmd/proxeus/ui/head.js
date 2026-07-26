(function(){
  var PREFIX = (window.__PROXEUS_ROUTE_PREFIX__ || '');
  var bp = PREFIX + '/backends';
  if (window.location.pathname === bp || window.location.pathname === bp + '/') {
    window.__proxeusBackends = true;
    var origPush = history.pushState;
    var origReplace = history.replaceState;
    history.pushState = function(s,t,u) { if (!window.__proxeusNavReady) return; return origPush.apply(this,arguments); };
    history.replaceState = function(s,t,u) { if (!window.__proxeusNavReady) return; return origReplace.apply(this,arguments); };
  }
})();
