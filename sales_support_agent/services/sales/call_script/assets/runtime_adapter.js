/* Anata call script runtime adapter.
 *
 * The call script was built for the claude.ai artifact runtime, which exposes
 * window.claude.use(name). This adapter provides the same surface inside the
 * agent so the script runs unchanged:
 *   - "user":   the signed-in agent user
 *   - "db":     the user's call reviews, stored in the agent database
 *   - "sample": Ask Claude, proxied through the agent (no key in the browser)
 *   - anything else (e.g. "mcp") resolves null, and the script hides it.
 * It also hydrates this lead's saved tracker into the script's storage keys
 * before the script starts, and saves changes back to the lead every few seconds.
 */
(function () {
  var CFG = window.__ANATA_CALL__ || {};
  var PER_CALL = ['ss-beliefs', 'ss-gap', 'ss-nums', 'ss-sel', 'ss-asks', 'ss-ai', 'ss-callmeta', 'ss-last'];

  /* ---- Hydrate this lead's tracker before the script reads storage ---- */
  try {
    PER_CALL.forEach(function (k) { localStorage.removeItem(k); });
    localStorage.removeItem('ss-calls');
    var st = CFG.state || {};
    Object.keys(st).forEach(function (k) {
      if (PER_CALL.indexOf(k) > -1) localStorage.setItem(k, JSON.stringify(st[k]));
    });
  } catch (e) { /* storage blocked: the tracker still works for this page view */ }

  function snapshot() {
    var out = {};
    PER_CALL.forEach(function (k) {
      var v = null;
      try { v = localStorage.getItem(k); } catch (e) { v = null; }
      if (v != null) { try { out[k] = JSON.parse(v); } catch (e) { /* skip unreadable key */ } }
    });
    return out;
  }

  function headers() {
    return { 'Content-Type': 'application/json', 'X-CSRF-Token': CFG.csrf || '' };
  }

  function apiError(status, message) {
    var code = status === 401 || status === 403 ? 'not_granted'
      : status === 429 ? 'rate_limited'
      : status === 503 ? 'unavailable' : 'failed';
    var err = new Error(message || ('Request failed (' + status + ')'));
    err.code = code; err.status = status;
    return err;
  }

  function api(path, opts) {
    opts = opts || {};
    return fetch(CFG.base + path, {
      method: opts.method || 'GET',
      headers: headers(),
      body: opts.body,
      signal: opts.signal,
      credentials: 'same-origin'
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!r.ok) throw apiError(r.status, j && (j.detail || j.error));
        return j;
      });
    });
  }

  /* ---- Save status pill + link back to the lead list ---- */
  var pill = null;
  function setStatus(state) {
    if (!pill) return;
    var txt = { saved: 'Saved', saving: 'Saving', error: 'Not saved, retrying', idle: 'Saved' }[state] || 'Saved';
    pill.querySelector('[data-save-state]').textContent = txt;
    pill.setAttribute('data-state', state);
  }
  function mountPill() {
    pill = document.createElement('div');
    pill.className = 'anata-callbar';
    pill.innerHTML = '<a href="' + CFG.base + '">All calls</a>'
      + '<span class="anata-callbar__lead"></span>'
      + '<span class="anata-callbar__state" data-save-state>Saved</span>';
    pill.querySelector('.anata-callbar__lead').textContent = CFG.label || '';
    var css = document.createElement('style');
    css.textContent = '.anata-callbar{position:fixed;left:max(12px,env(safe-area-inset-left,0px));bottom:max(12px,env(safe-area-inset-bottom,0px));z-index:9999;display:flex;gap:10px;align-items:center;padding:6px 12px;border-radius:999px;background:#2B3644;color:#fff;font:600 12px/1.2 Inter,system-ui,sans-serif;box-shadow:0 4px 18px rgba(0,0,0,.18)}'
      + '.anata-callbar a{color:#fff;text-decoration:underline}'
      + '.anata-callbar__lead{opacity:.85;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
      + '.anata-callbar__state{padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.14)}'
      + '.anata-callbar[data-state=error] .anata-callbar__state{background:#C0392B}';
    document.head.appendChild(css);
    document.body.appendChild(pill);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mountPill); else mountPill();

  /* ---- Save this lead's tracker back to the agent ---- */
  var last = JSON.stringify(snapshot());
  var inflight = false;
  function push(useBeacon) {
    var cur = snapshot(), s = JSON.stringify(cur);
    if (s === last || !CFG.trackerId) return;
    var body = JSON.stringify({ state: cur });
    if (useBeacon && navigator.sendBeacon) {
      var ok = navigator.sendBeacon(
        CFG.base + '/' + CFG.trackerId + '/state?csrf=' + encodeURIComponent(CFG.csrf || ''),
        new Blob([body], { type: 'application/json' })
      );
      if (ok) last = s;
      return;
    }
    if (inflight) return;
    inflight = true; setStatus('saving');
    api('/' + CFG.trackerId + '/state', { method: 'PUT', body: body })
      .then(function () { last = s; setStatus('saved'); })
      .catch(function () { setStatus('error'); })
      .then(function () { inflight = false; });
  }
  setInterval(function () { push(false); }, 2500);
  window.addEventListener('pagehide', function () { push(true); });
  document.addEventListener('visibilitychange', function () {
    if (document.visibilityState === 'hidden') push(true);
  });

  /* ---- Capability namespaces ---- */
  var userNs = {
    id: function () { return Promise.resolve(CFG.user && CFG.user.email || ''); },
    me: function () { return Promise.resolve({ id: CFG.user.email, name: CFG.user.name || '', email: CFG.user.email }); },
    isOwner: function () { return true; },
    canEdit: function () { return true; },
    can: function () { return true; }
  };

  var dbNs = {
    collection: function () {
      return {
        get: function () {
          return api('/api/reviews').then(function (j) {
            return { docs: (j.reviews || []).map(function (r) { return { id: r.id, data: function () { return r; } }; }) };
          });
        },
        doc: function (id) {
          var path = '/api/reviews/' + encodeURIComponent(id);
          return {
            set: function (obj) { return api(path, { method: 'PUT', body: JSON.stringify({ review: obj, trackerId: CFG.trackerId }) }); },
            delete: function () { return api(path, { method: 'DELETE' }); },
            get: function () {
              return api(path).then(function (j) {
                return { exists: !!j.review, id: id, data: function () { return j.review; } };
              });
            }
          };
        }
      };
    }
  };

  function parseJSON(text) {
    var t = String(text || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '');
    try { return JSON.parse(t); } catch (e) { /* fall through */ }
    var a = t.search(/[\[{]/);
    var b = Math.max(t.lastIndexOf('}'), t.lastIndexOf(']'));
    if (a > -1 && b > a) return JSON.parse(t.slice(a, b + 1));
    throw apiError(422, 'Claude did not return JSON.');
  }

  function sample(input, opts) {
    opts = opts || {};
    return api('/api/sample', {
      method: 'POST',
      body: JSON.stringify({ input: input, tier: opts.modelTier || 'default', json: !!opts.__json }),
      signal: opts.signal
    }).then(function (j) {
      var text = j.text || '';
      if (typeof opts.onText === 'function') {
        try { opts.onText({ text: text, delta: text }); } catch (e) { /* display callback only */ }
      }
      return { text: text, truncated: !!j.truncated };
    });
  }
  sample.json = function (input, opts) {
    var o = Object.assign({}, opts || {}, { __json: true });
    return sample(input, o).then(function (r) { return parseJSON(r.text); });
  };
  sample.limits = function () { return Promise.resolve({ images: false }); };

  window.claude = {
    use: function (name) {
      if (name === 'user') return Promise.resolve(userNs);
      if (name === 'db') return Promise.resolve(dbNs);
      if (name === 'sample') return Promise.resolve(CFG.aiEnabled ? sample : null);
      return Promise.resolve(null);
    }
  };
})();
