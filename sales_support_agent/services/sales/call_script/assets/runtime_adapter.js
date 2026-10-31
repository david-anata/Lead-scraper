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
  var PER_CALL = ['ss-beliefs', 'ss-gap', 'ss-nums', 'ss-sel', 'ss-asks', 'ss-ai', 'ss-callmeta', 'ss-last', 'ss-live', 'ss-prep'];

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
  function prepText(value) {
    if (value == null) return '';
    return String(value).trim();
  }
  function prepKnown(value) {
    var t = prepText(value);
    return t && !/^unknown\.?$/i.test(t) ? t : '';
  }
  var PREPARED = /^prepared(,\s*not raised by them)?\s*:\s*/i;
  function mountPrepCss() {
    if (document.getElementById('call-prep-css')) return;
    var css = document.createElement('style');
    css.id = 'call-prep-css';
    css.textContent = ''
      + '#call-prep .prep-meta{display:flex;flex-wrap:wrap;gap:8px;margin:-4px 0 16px}'
      + '#call-prep .prep-chip{display:inline-block;padding:3px 10px;border-radius:999px;border:1px solid var(--line);font-size:13px;font-weight:600;color:var(--muted)}'
      + '#call-prep .prep-facts{display:grid;grid-template-columns:max-content 1fr;gap:6px 16px;margin:0 0 6px;font-size:14.5px;line-height:1.45}'
      + '#call-prep .prep-facts dt{font-weight:700;color:var(--muted);margin:0}'
      + '#call-prep .prep-facts dd{margin:0;max-width:70ch}'
      + '#call-prep details{border-top:1px solid var(--line);padding:10px 0 4px;margin-top:10px}'
      + '#call-prep summary{cursor:pointer;font-weight:700;font-size:16px;list-style-position:inside}'
      + '#call-prep details ol,#call-prep details ul{margin:8px 0 4px;padding-left:1.4em}'
      + '#call-prep details li{margin:5px 0;font-size:14.5px;line-height:1.45}'
      + '#call-prep .prep-obj{margin:10px 0 0;font-size:14.5px;line-height:1.45}'
      + '#call-prep .prep-obj b{display:block}'
      + '#call-prep .prep-tag{display:inline-block;margin-right:6px;padding:1px 7px;border-radius:999px;background:var(--line);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;vertical-align:1px}'
      + '#call-prep .prep-unknown{margin:12px 0 0;font-size:13px;color:var(--muted)}'
      + '@media (max-width:560px){#call-prep .prep-facts{grid-template-columns:1fr}#call-prep .prep-facts dt{margin-top:6px}}';
    document.head.appendChild(css);
  }
  /* A model sometimes returns one paragraph where a list belongs. Split lines
     or sentences. Never walk the string one character at a time. */
  function asList(value) {
    if (Array.isArray(value)) {
      if (value.length && value.every(function (item) { return typeof item === 'string' && item.length === 1; })) {
        return asList(value.join(''));
      }
      return value.filter(function (item) { return item != null && item !== ''; });
    }
    if (typeof value !== 'string') return [];
    var text = value.trim();
    if (!text) return [];
    var lines = text.split(/\r?\n+/).map(function (line) {
      return line.replace(/^[\s\-•*]+/, '').trim();
    }).filter(Boolean);
    if (lines.length > 1) return lines;
    var sentences = [];
    var buf = '';
    for (var i = 0; i < text.length; i++) {
      buf += text.charAt(i);
      var end = /[.!?]/.test(text.charAt(i)) && (i === text.length - 1 || /\s/.test(text.charAt(i + 1)));
      if (end) {
        if (buf.trim()) sentences.push(buf.trim());
        buf = '';
        while (i + 1 < text.length && /\s/.test(text.charAt(i + 1))) i++;
      }
    }
    if (buf.trim()) sentences.push(buf.trim());
    return sentences.length > 1 ? sentences : [text];
  }
  function prepBrief(prep) {
    if (!prep) return '';
    var sections = prep.sections || {};
    var links = prep.links || {};
    var site = links.website || {};
    var amazon = links.amazon || {};
    var lines = [];
    if (sections.who) lines.push(sections.who);
    if (site.verified && site.url) lines.push('Website: ' + site.url);
    if (amazon.line) lines.push(amazon.line);
    else lines.push('Amazon presence: not verified');
    asList(sections.discovery).forEach(function (step) {
      if (step && typeof step === 'object' && step.known && step.known.indexOf('Unknown') !== 0) {
        lines.push((step.id || '') + ' ' + (step.title || '') + ': ' + step.known);
      }
    });
    return lines.join('\n');
  }
  window.__anataPrepBrief = prepBrief;
  function mountPrep() {
    var prep = CFG.state && CFG.state['ss-prep'];
    var main = document.getElementById('main');
    if (!prep || !main || document.getElementById('call-prep')) return;
    mountPrepCss();
    var sections = prep.sections || {};
    var panel = document.createElement('section');
    panel.className = 'sec';
    panel.id = 'call-prep';
    panel.setAttribute('data-title', 'Prep');
    var heading = document.createElement('h2');
    heading.textContent = 'Prep';
    panel.appendChild(heading);

    var meta = document.createElement('div');
    meta.className = 'prep-meta';
    [prepKnown(prep.when), prepKnown(prep.purpose)].forEach(function (t) {
      if (!t) return;
      var chip = document.createElement('span');
      chip.className = 'prep-chip';
      chip.textContent = t;
      meta.appendChild(chip);
    });
    if (meta.childNodes.length) panel.appendChild(meta);

    /* Only show facts we actually have; the gaps go in one line at the end. */
    var facts = document.createElement('dl');
    facts.className = 'prep-facts';
    [['Who', sections.who], ['How they came in', sections.how_they_came_in],
     ['Likely needs', sections.likely_needs], ['Next step', sections.next_step]].forEach(function (row) {
      var t = prepKnown(row[1]);
      if (!t) return;
      var dt = document.createElement('dt'); dt.textContent = row[0];
      var dd = document.createElement('dd'); dd.textContent = t;
      facts.appendChild(dt); facts.appendChild(dd);
    });
    if (facts.childNodes.length) panel.appendChild(facts);

    function addGroup(label, build) {
      var box = document.createElement('details');
      box.open = true;
      var sum = document.createElement('summary');
      sum.textContent = label;
      box.appendChild(sum);
      if (build(box)) panel.appendChild(box);
    }
    var links = prep.links || {};
    var site = links.website || {};
    var amazon = links.amazon || {};
    if (site.verified && site.url) {
      var siteDt = document.createElement('dt'); siteDt.textContent = 'Website';
      var siteDd = document.createElement('dd');
      var siteA = document.createElement('a');
      siteA.href = site.url; siteA.textContent = site.url; siteA.rel = 'noopener'; siteA.target = '_blank';
      siteDd.appendChild(siteA);
      facts.appendChild(siteDt); facts.appendChild(siteDd);
    }
    var amDt = document.createElement('dt'); amDt.textContent = 'Amazon';
    var amDd = document.createElement('dd');
    if (amazon.verified && amazon.urls && amazon.urls.length) {
      amazon.urls.forEach(function (href, i) {
        if (i) amDd.appendChild(document.createTextNode(' '));
        var a = document.createElement('a');
        a.href = href; a.textContent = href; a.rel = 'noopener'; a.target = '_blank';
        amDd.appendChild(a);
      });
    } else {
      amDd.textContent = 'Amazon presence: not verified';
    }
    facts.appendChild(amDt); facts.appendChild(amDd);
    if (!facts.parentNode && facts.childNodes.length) panel.appendChild(facts);

    function addList(label, items, ordered) {
      addGroup(label, function (box) {
        var list = document.createElement(ordered ? 'ol' : 'ul');
        asList(items).forEach(function (item) {
          var t = prepKnown(typeof item === 'object' && item
            ? ((item.id ? item.id + ' ' : '') + (item.title ? item.title + '. ' : '') + (item.known || '') + (item.angle ? ' Angle: ' + item.angle : ''))
            : item);
          if (!t) return;
          var li = document.createElement('li');
          li.textContent = t;
          list.appendChild(li);
        });
        if (!list.childNodes.length) return false;
        box.appendChild(list);
        return true;
      });
    }
    var steps = asList(sections.discovery);
    if (steps.length && typeof steps[0] === 'object') addList('Discovery', steps, false);
    else addList('Discovery', sections.discovery_questions, true);
    addList('Trial pitch', sections.trial_pitch, false);
    addGroup('Objections', function (box) {
      var n = 0;
      asList(sections.objections).forEach(function (item) {
        if (!item || typeof item !== 'object') return;
        var q = prepKnown(item.objection), ans = prepKnown(item.answer);
        if (!q && !ans) return;
        var div = document.createElement('div');
        div.className = 'prep-obj';
        var b = document.createElement('b');
        if (PREPARED.test(q)) {
          var tag = document.createElement('span');
          tag.className = 'prep-tag';
          tag.textContent = 'Prepared';
          tag.title = 'Not raised by them yet';
          b.appendChild(tag);
          q = q.replace(PREPARED, '');
        }
        b.appendChild(document.createTextNode(q));
        div.appendChild(b);
        if (ans) div.appendChild(document.createTextNode(ans));
        box.appendChild(div);
        n += 1;
      });
      return n > 0;
    });
    var unknowns = asList(prep.unknowns).map(prepText).filter(Boolean);
    if (unknowns.length) {
      var u = document.createElement('p');
      u.className = 'prep-unknown';
      u.textContent = 'Not found yet: ' + unknowns.join(', ');
      panel.appendChild(u);
    }
    var hero = main.querySelector('.hero');
    if (hero) main.insertBefore(panel, hero.nextSibling);
    else main.insertBefore(panel, main.firstChild);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { mountPill(); mountPrep(); });
  } else {
    mountPill();
    mountPrep();
  }

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

/* ---------------------------------------------------------------------------
 * Live call module: Start live call, Tape extension bridge, live state polling, coach
 * updates applied to the tracker, End call with automatic review and the
 * after-call steps. Renders into [data-live] at the top of the Talk flow tab.
 * ------------------------------------------------------------------------- */
(function () {
  var CFG = window.__ANATA_CALL__ || {};
  if (!CFG.trackerId) return;
  var BASE = CFG.base + '/' + CFG.trackerId;
  var POLL_MS = 2000;
  var EXT_MIN = '1.1.0';
  var OFFERS = {
    starter: 'Brand Starter, $99/mo',
    growth: 'Brand Growth, $199/mo',
    pro: 'Brand Pro, $399/mo',
    full: 'Full service, $3,000/mo + 5% over baseline'
  };
  var KIND = { ask: 'Ask next', handle: 'Handle it', tiedown: 'Tie it down', moveon: 'Move on', pitch: 'Pitch now', close: 'Close', temperature: 'Temperature check' };
  function prepBrief(prep) { try { return window.__anataPrepBrief ? window.__anataPrepBrief(prep) : ''; } catch (e) { return ''; } }
  var BELIEFS = ['Pain', 'Doubt', 'Cost', 'Desire', 'Support', 'Trust', 'Money'];

  var st = {
    session: null, lines: [], lastLineId: 0, rev: 0, patch: null, coachRunning: false,
    polling: null, failures: 0, ext: { present: false, version: '', status: '' },
    pingTimer: null, busy: false, error: '', afterCall: null, reviewState: '', pitchAt: null,
    justEnded: false
  };

  function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]; }); }
  function $(sel, r) { return (r || document).querySelector(sel); }
  function $$(sel, r) { return Array.prototype.slice.call((r || document).querySelectorAll(sel)); }

  function api(path, opts) {
    opts = opts || {};
    return fetch(BASE + path, {
      method: opts.method || 'GET',
      headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': CFG.csrf || '' },
      body: opts.body ? JSON.stringify(opts.body) : undefined,
      credentials: 'same-origin'
    }).then(function (r) {
      return r.json().catch(function () { return {}; }).then(function (j) {
        if (!r.ok) { var e = new Error((j && (j.detail || j.error)) || ('Request failed (' + r.status + ')')); e.status = r.status; throw e; }
        return j;
      });
    });
  }

  /* ---- Per-lead live memory (locks and coach quotes), saved with the tracker ---- */
  function liveMem() {
    var m = null;
    try { m = JSON.parse(localStorage.getItem('ss-live') || 'null'); } catch (e) { m = null; }
    if (!m || typeof m !== 'object') m = {};
    m.locks = m.locks || {}; m.locks.beliefs = m.locks.beliefs || {};
    m.coach = m.coach || {}; m.coach.beliefs = m.coach.beliefs || {};
    return m;
  }
  function saveMem(m) { try { localStorage.setItem('ss-live', JSON.stringify(m)); } catch (e) { /* storage blocked */ } }

  /* ---- Tape extension bridge ---- */
  function verGte(a, b) {
    var x = String(a || '0').split('.').map(Number), y = String(b).split('.').map(Number);
    for (var i = 0; i < 3; i++) { if ((x[i] || 0) !== (y[i] || 0)) return (x[i] || 0) > (y[i] || 0); }
    return true;
  }
  function toExt(type, extra) {
    var msg = { source: 'anata-call', type: type };
    if (extra) for (var k in extra) msg[k] = extra[k];
    window.postMessage(msg, location.origin);
  }
  window.addEventListener('message', function (e) {
    if (e.source !== window || !e.data || e.data.source !== 'tape-ext') return;
    var d = e.data;
    if (d.type === 'PONG' || d.type === 'LIVE_STATUS') {
      st.ext.present = true;
      if (d.version) st.ext.version = d.version;
      if (st.pingTimer) { clearTimeout(st.pingTimer); st.pingTimer = null; }
    }
    if (d.type === 'LIVE_STATUS') {
      st.ext.status = d.status || '';
      st.error = d.status === 'error' ? (d.error || 'The Tape extension reported a problem.') : st.error;
      if (d.status === 'live' && !st.polling) startPolling();
    }
    render();
  });
  function ping() {
    st.ext.present = false;
    if (st.pingTimer) clearTimeout(st.pingTimer);
    st.pingTimer = setTimeout(function () { st.pingTimer = null; render(); }, 1500);
    toExt('PING');
  }
  function extReady() { return st.ext.present && verGte(st.ext.version, EXT_MIN); }

  /* ---- Coach patch -> tracker ---- */
  function markCoachNumbers(keys) {
    (keys || []).forEach(function (k) {
      $$('[data-num="' + k + '"]').forEach(function (el) { el.classList.add('coachfill'); el.title = 'Filled from the call. Type to change it.'; });
    });
  }
  function paintBeliefQuotes() {
    var m = liveMem();
    $$('.meter .bt[data-b]').forEach(function (b) {
      var q = m.coach.beliefs[b.dataset.b];
      var locked = Object.prototype.hasOwnProperty.call(m.locks.beliefs, b.dataset.b);
      b.classList.toggle('coach', !!q && !locked && b.getAttribute('aria-pressed') === 'true');
      if (q) b.title = 'Heard: "' + q + '"' + (locked ? ' (you set this one)' : '');
    });
  }
  function paintSections(p) {
    var secs = (p && p.sections) || {};
    $$('nav a[data-sid]').forEach(function (a) {
      var s = secs[a.dataset.sid];
      a.classList.toggle('live-done', !!(s && s.done));
      if (s && s.done && s.quote) a.title = 'Covered: "' + s.quote + '"';
    });
  }
  function applyPatch(p) {
    if (!p || !window.SS) return;
    var SS = window.SS, m = liveMem();
    var said = [];
    BELIEFS.forEach(function (b) {
      var v = p.beliefs && p.beliefs[b];
      if (!v) return;
      if (v.quote) m.coach.beliefs[b] = v.quote;
      if (v.status === 'said' && !Object.prototype.hasOwnProperty.call(m.locks.beliefs, b)) said.push(b);
    });
    saveMem(m);
    if (SS.applyCall) {
      var out = SS.applyCall({ beliefs: said, nums: p.numbers || {}, quotes: p.quotes || [], personalGoal: p.personalGoal || '' });
      markCoachNumbers(out && out.nums);
    }
    if (SS.applySel) SS.applySel({ pains: p.pains || [], objs: p.objs || [], res: p.res || [] });
    if (p.pitch && p.pitch.ready && !st.pitchAt) st.pitchAt = Date.now();
    paintSections(p);
    paintBeliefQuotes();
  }

  /* Rep clicks a belief: from then on the coach leaves that belief alone */
  document.addEventListener('click', function (e) {
    var b = e.target && e.target.closest ? e.target.closest('.meter .bt[data-b]') : null;
    if (!b) return;
    setTimeout(function () {
      var m = liveMem();
      if (!st.session && !m.coach.beliefs[b.dataset.b]) return;
      m.locks.beliefs[b.dataset.b] = b.getAttribute('aria-pressed') === 'true';
      saveMem(m); paintBeliefQuotes();
    }, 0);
  }, true);
  document.addEventListener('input', function (e) {
    if (e.target && e.target.classList && e.target.classList.contains('coachfill')) {
      e.target.classList.remove('coachfill'); e.target.removeAttribute('title');
    }
  }, true);

  /* ---- Polling ---- */
  function startPolling() {
    if (st.polling) return;
    st.polling = setInterval(poll, POLL_MS);
    poll();
  }
  function stopPolling() { if (st.polling) { clearInterval(st.polling); st.polling = null; } }
  function poll() {
    return api('/live/state?after=' + st.lastLineId + '&rev=' + st.rev).then(function (j) {
      st.failures = 0;
      st.session = j.session || null;
      (j.lines || []).forEach(function (l) { st.lines.push(l); });
      if (st.lines.length > 400) st.lines = st.lines.slice(-400);
      if (typeof j.lastLineId === 'number') st.lastLineId = Math.max(st.lastLineId, j.lastLineId);
      st.coachRunning = !!j.coachRunning;
      if (j.coach && j.coach.rev > st.rev) { st.rev = j.coach.rev; st.patch = j.coach.patch || null; applyPatch(st.patch); }
      if (!st.session || st.session.status === 'ended') stopPolling();
      render();
    }).catch(function () { st.failures++; render(); });
  }

  /* ---- Actions ---- */
  function goLive() {
    if (st.busy) return;
    st.busy = true; st.error = ''; st.afterCall = null; st.reviewState = ''; st.justEnded = false; render();
    var ctx = '';
    try { ctx = (window.SS && window.SS.coachContext) ? window.SS.coachContext() : ''; } catch (e) { ctx = ''; }
    var brief = prepBrief(CFG.state && CFG.state['ss-prep']);
    if (brief) {
      ctx += '\n\nPREFILLED LEAD FACTS (already on the tracker, not yet said on this call; do not mark a belief said from these):\n' + brief;
    }
    api('/live', { method: 'POST', body: { coachContext: ctx.slice(0, 120000), label: CFG.label || '' } }).then(function (j) {
      st.session = { id: j.sessionId, status: 'pending', startedAt: j.startedAt, consentAt: null, endedAt: null };
      st.lines = []; st.lastLineId = 0; st.rev = 0; st.patch = null; st.pitchAt = null;
      toExt('LIVE_ARM', { session: { sessionId: j.sessionId, token: j.token, relayUrl: j.relayUrl, label: CFG.label || '', trackerUrl: location.href } });
      st.busy = false;
      startPolling();
      var tab = document.getElementById('tab-flow'); if (tab) tab.click();
    }).catch(function (e) { st.busy = false; st.error = e.message; render(); });
  }
  function rearm() {
    var rearmCtx = '';
    try { rearmCtx = window.SS && window.SS.coachContext ? window.SS.coachContext() : ''; } catch (e) { rearmCtx = ''; }
    var rearmBrief = prepBrief(CFG.state && CFG.state['ss-prep']);
    if (rearmBrief) rearmCtx += '\n\nPREFILLED LEAD FACTS (already on the tracker, not yet said on this call):\n' + rearmBrief;
    api('/live', { method: 'POST', body: { coachContext: rearmCtx.slice(0, 120000), label: CFG.label || '' } }).then(function (j) {
      toExt('LIVE_ARM', { session: { sessionId: j.sessionId, token: j.token, relayUrl: j.relayUrl, label: CFG.label || '', trackerUrl: location.href } });
      st.error = ''; render();
    }).catch(function (e) { st.error = e.message; render(); });
  }
  function consent() {
    api('/live/consent', { method: 'POST', body: {} }).then(function (j) {
      if (st.session) st.session.consentAt = j.consentAt; render();
    }).catch(function (e) { st.error = e.message; render(); });
  }
  function endCall() {
    if (st.busy) return;
    st.busy = true; render();
    toExt('LIVE_STOP');
    api('/live/end', { method: 'POST', body: {} }).then(function (j) {
      st.busy = false; stopPolling(); st.justEnded = true;
      if (st.session) { st.session.status = 'ended'; st.session.endedAt = j.endedAt; }
      var text = (j.transcript || '').trim();
      if (!text) { st.reviewState = 'empty'; render(); return; }
      if (!/^Prospect:/m.test(text)) { st.reviewState = 'noprospect'; render(); return; }
      var ok = window.SS && window.SS.liveReview ? window.SS.liveReview(text, 'Live call: ' + (CFG.label || 'prospect')) : false;
      st.reviewState = ok ? 'running' : 'unavailable';
      render();
    }).catch(function (e) { st.busy = false; st.error = e.message; render(); });
  }
  function offerKey() { return (st.patch && st.patch.pitch && st.patch.pitch.offer) || 'none'; }
  function runAfterCall(analysis) {
    st.reviewState = 'saving'; st.lastAnalysis = analysis; render();
    api('/after-call', { method: 'POST', body: { analysis: analysis, offer: offerKey() } }).then(function (j) {
      st.afterCall = j.steps || {}; st.reviewState = 'done'; render();
    }).catch(function (e) { st.reviewState = 'failed'; st.error = e.message; render(); });
  }
  document.addEventListener('ss:aianalyzed', function (e) {
    if (!st.justEnded) return;
    runAfterCall(e.detail && e.detail.analysis);
  });

  /* ---- Rendering ---- */
  function mmss(ms) {
    if (!(ms >= 0)) return '0:00';
    var s = Math.floor(ms / 1000), m = Math.floor(s / 60);
    return m + ':' + String(s % 60).padStart(2, '0');
  }
  function since(iso) { var t = Date.parse(iso || ''); return isNaN(t) ? 0 : Date.now() - t; }
  function discoveryMin() {
    if (!st.session) return 0;
    var startT = Date.parse(st.session.firstAudioAt || st.session.startedAt || '');
    if (isNaN(startT)) return 0;
    var endT = st.pitchAt || Date.now();
    return Math.max(0, Math.floor((endT - startT) / 60000));
  }
  function extLine() {
    if (st.pingTimer) return '<span class="lv-pill">Checking for Tape</span>';
    if (extReady()) return '<span class="lv-pill ok">Tape ' + esc(st.ext.version) + ' connected</span>';
    if (st.ext.present) return '<span class="lv-pill warn">Tape ' + esc(st.ext.version || '') + ' needs an update</span>';
    return '<span class="lv-pill warn">Tape extension not found</span>';
  }
  function installHelp() {
    return '<p class="hint">Install or update the Tape extension (v' + EXT_MIN + '+), then reload this page. On a call: open your Google Meet tab, click the Tape icon (or Alt+Shift+T), and press <b>Start live call</b>.</p>';
  }
  function slideBtns(p) {
    var nums = (p.slides || []).filter(function (n) { return n > 0; }).slice(0, 4);
    var T = (window.SS && window.SS.deck && window.SS.deck.titles) ? window.SS.deck.titles() : {};
    if (!nums.length) return '<button type="button" class="lv-btn sec" data-lvgo="pricing">Open pricing</button>';
    return nums.map(function (n) { return '<button type="button" class="lv-btn sec" data-lvslide="' + n + '" title="' + esc(T[n] || '') + '">Slide ' + n + '</button>'; }).join('');
  }
  function caseName(k) {
    var C = window.SS && window.SS.meta && window.SS.meta.CASES;
    return C && C[k] ? C[k].n + ': ' + C[k].r : '';
  }
  /* Hold each next move on screen; when a new one arrives, show a 5-second
     fill bar at the bottom of the card, then switch. Keeps the card readable. */
  var MOVE_HOLD_MS = 5000;
  function moveKey(m) { return m ? (m.kind || '') + '|' + (m.say || '') : ''; }
  function stagedMove(incoming) {
    if (!incoming || !incoming.say) return st.moveShown || incoming;
    if (!st.moveShown) { st.moveShown = incoming; st.movePending = null; return incoming; }
    if (moveKey(incoming) === moveKey(st.moveShown)) { st.moveShown = incoming; st.movePending = null; return incoming; }
    if (!st.movePending || moveKey(st.movePending) !== moveKey(incoming)) {
      if (!st.movePending) st.movePendingAt = Date.now();
      st.movePending = incoming;
      clearTimeout(st.moveTimer);
      st.moveTimer = setTimeout(function () { render(); }, Math.max(0, MOVE_HOLD_MS - (Date.now() - st.movePendingAt)) + 20);
    }
    if (Date.now() - st.movePendingAt >= MOVE_HOLD_MS) { st.moveShown = st.movePending; st.movePending = null; }
    return st.moveShown;
  }
  function startBtn() {
    return '<div class="lv-btns lv-start"><button type="button" class="lv-btn" data-lvgolive' + (st.busy ? ' disabled' : '') + '>' + (st.busy ? 'Starting' : 'Start live call') + '</button></div>';
  }
  function liveHTML() {
    var s = st.session, p = st.patch || {}, h = '';
    var live = s.status === 'live';
    h += '<div class="lv-head"><span class="lv-dot' + (live ? ' on' : '') + '" aria-hidden="true"></span><h3>' + (live ? 'Live' : 'Armed') + '</h3>';
    h += '<span class="lv-timer" data-lvtimer>' + mmss(since(s.firstAudioAt || s.startedAt)) + '</span>';
    if (live) { var dm = discoveryMin(); h += '<span class="lv-pill ' + (dm >= 20 ? 'ok' : '') + '">Discovery ' + dm + ' of 20 min</span>'; }
    if (st.coachRunning) h += '<span class="lv-pill">Coach reading</span>';
    if (st.failures >= 2) h += '<span class="lv-pill bad">Live paused, retrying</span>';
    h += '</div>';
    if (!live) {
      h += startBtn();
      h += '<p>Armed. Start live call sends this session to the Tape extension. If the call does not begin, switch to your Meet tab, click the Tape icon (or Alt+Shift+T), and confirm there. Chrome may require that click before it can hear the call.</p>';
      if (!extReady()) h += installHelp();
      h += '<div class="lv-btns"><button type="button" class="lv-btn sec" data-lvrearm>Send to Tape again</button><button type="button" class="lv-btn sec" data-lvend>Cancel</button></div>';
      return h;
    }
    if (!s.consentAt) h += '<div class="lv-card consent"><div class="k">Consent</div><div class="say">Say: "Quick note, this call is recorded."</div><div class="lv-btns"><button type="button" class="lv-btn" data-lvconsent>Said it</button></div></div>';
    var heardProspect = st.lines.some(function (l) { return l.speaker === 'prospect'; });
    var heardRep = st.lines.some(function (l) { return l.speaker === 'rep'; });
    if (heardRep && !heardProspect && since(s.firstAudioAt || s.startedAt) > 20000) {
      h += '<div class="lv-warn">Only hearing you so far. The prospect\'s side comes from the Meet tab\'s audio, so it fills in once someone else talks on the call.</div>';
    }
    (p.warnings || []).slice(0, 3).forEach(function (w) { h += '<div class="lv-warn">' + esc(w) + '</div>'; });
    var nm = stagedMove(p.nextMove), pend = st.movePending;
    if (nm && nm.say) {
      h += '<div class="lv-card lv-next"><div class="k">' + esc(KIND[nm.kind] || 'Next move') + '</div><div class="say">' + esc(nm.say) + '</div><div class="why">' + esc(nm.why || '') + '</div>' +
        '<div class="lv-btns" style="margin-top:6px">' + (nm.section ? '<button type="button" class="lv-btn sec" data-lvgo="' + esc(nm.section) + '">Open the script here</button>' : '') + '</div>' +
        '<div class="lv-up" aria-live="off">' + (pend ? 'New move coming up' : '') + '</div>' +
        (pend ? '<div class="lv-bar" role="progressbar" aria-label="Switching to the next move in 5 seconds"><i style="animation-delay:-' + Math.min(MOVE_HOLD_MS, Date.now() - st.movePendingAt) + 'ms"></i></div>' : '') + '</div>';
    } else {
      h += '<div class="lv-card lv-next"><div class="k">Next move</div><div class="why">Listening. The coach updates every 10 to 15 seconds once the conversation starts.</div></div>';
    }
    if (p.pitch && p.pitch.ready) {
      h += '<div class="lv-card pitch"><div class="k">Ready to pitch</div><div class="say">' + esc(OFFERS[p.pitch.offer] || 'Anata Intelligence') + '</div>' +
        (p.pitch.bridge ? '<p>' + esc(p.pitch.bridge) + '</p>' : '') +
        (p.pitch.case && caseName(p.pitch.case) ? '<p class="why">Proof: ' + esc(caseName(p.pitch.case)) + '</p>' : '') +
        '<div class="lv-btns" style="margin-top:6px">' + slideBtns(p.pitch) + '</div></div>';
    }
    var t = p.temperature;
    if (t && t.asked) {
      var good = t.quality === 'clear' && t.score >= 8;
      h += '<div class="lv-card ' + (good ? 'pitch' : 'guard') + '"><div class="k">Temperature check</div><div class="say">' + (t.score != null ? esc(t.score) + ' of 10, ' : '') + esc(t.quality === 'clear' ? 'clear answer' : 'vague answer') + '</div>' +
        (good ? '<p>Move to the close: connect Seller Central on the call.</p>' : '<p>' + esc(t.followUp || 'Ask what would make it a 10, then handle the objection.') + '</p><div class="lv-btns"><button type="button" class="lv-btn sec" data-lvraised>Back to objections</button></div>') + '</div>';
    }
    var th = p.three || {};
    h += '<div class="lv-row"><span class="lv-pill ' + (th.problem ? 'ok' : '') + '">Problem</span><span class="lv-pill ' + (th.gap ? 'ok' : '') + '">Gap</span><span class="lv-pill ' + (th.cost ? 'ok' : '') + '">Cost of waiting</span></div>';
    var m = liveMem(), heard = BELIEFS.filter(function (b) { return m.coach.beliefs[b]; });
    if (heard.length) h += '<div><div class="k" style="font-size:11.5px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.04em">Beliefs heard</div><ul class="lv-list">' + heard.map(function (b) { return '<li><b>' + esc(b) + ':</b> "' + esc(m.coach.beliefs[b]) + '"</li>'; }).join('') + '</ul></div>';
    var td = (p.tieDowns || []).filter(function (x) { return x && x.line; });
    if (td.length) h += '<div><div class="k" style="font-size:11.5px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.04em">Tie-downs</div><ul class="lv-list">' + td.map(function (x) { return '<li>"' + esc(x.line) + '" <span class="lv-pill' + ((x.uses || 0) >= 5 ? ' ok' : '') + '">' + esc(x.uses || 0) + ' of 6</span></li>'; }).join('') + '</ul></div>';
    var last = st.lines.slice(-3);
    if (last.length) h += '<ul class="lv-list lv-lines">' + last.map(function (l) { return '<li><b>' + (l.speaker === 'rep' ? 'You' : 'Them') + ':</b> ' + esc(l.text) + '</li>'; }).join('') + '</ul>';
    h += '<div class="lv-btns"><button type="button" class="lv-btn end" data-lvend>End call</button></div>';
    return h;
  }
  function stepLine(name, r) {
    r = r || {};
    var ok = r.ok === true;
    var label;
    if (r.ok === undefined) label = 'Pending';
    else if (!r.ok) label = r.error || 'Failed';
    else if (r.skipped) label = 'Skipped: ' + (r.reason || '');
    else if (r.note) label = r.note;
    else if (name === 'ClickUp') label = (r.created ? 'Task created' : 'Task updated') + (r.status ? ', status "' + r.status + '"' : '');
    else if (name === 'Recap') label = 'Gmail draft ready' + (r.to ? ' for ' + r.to : '');
    else if (name === 'Day-10 review') label = 'ClickUp task due ' + (r.due || 'in 10 days');
    else label = 'Done';
    var url = r.url || (r.taskId ? 'https://app.clickup.com/t/' + r.taskId : '') || (r.draftId ? 'https://mail.google.com/mail/u/0/#drafts' : '');
    var link = url ? ' <a href="' + esc(url) + '" target="_blank" rel="noopener">Open</a>' : '';
    return '<li><span><b>' + esc(name) + ':</b> ' + esc(label) + link + '</span>' + (ok ? '<span class="lv-pill ok">Done</span>' : '<button type="button" class="lv-btn sec" data-lvafter>Retry</button>') + '</li>';
  }
  function endedHTML() {
    var h = '<div class="lv-head"><span class="lv-dot" aria-hidden="true"></span><h3>Call ended</h3></div>';
    if (st.reviewState === 'running') h += '<p>Scoring the call from the transcript. The review panel shows progress.</p>';
    else if (st.reviewState === 'saving') h += '<p>Updating ClickUp, drafting the recap and setting the day-10 review.</p>';
    else if (st.reviewState === 'empty') h += '<p>No transcript was captured for this call, so there is nothing to review.</p>';
    else if (st.reviewState === 'noprospect') h += '<p>Only your side was captured, so there was nothing to review and nothing was sent to ClickUp. For a solo test, join the Meet from your phone and play the prospect there.</p>';
    else if (st.reviewState === 'unavailable') h += '<p>Ask Claude is off on this agent, so the automatic review can\'t run. Use End call &amp; review to log the call by hand.</p>';
    else if (st.reviewState === 'failed') h += '<p>The after-call steps didn\'t finish.</p><div class="lv-btns"><button type="button" class="lv-btn" data-lvafter>Retry</button></div>';
    if (st.afterCall) {
      h += '<ul class="lv-list lv-steps">' + stepLine('Review', { ok: true, note: 'Saved to your coaching dashboard' }) + stepLine('ClickUp', st.afterCall.clickup) + stepLine('Recap', st.afterCall.recap) + stepLine('Day-10 review', st.afterCall.day10) + '</ul>';
    }
    h += '<div class="lv-btns"><button type="button" class="lv-btn sec" data-lvnew>Start live call</button></div>';
    return h;
  }
  function idleHTML() {
    var h = '<div class="lv-head"><span class="lv-dot" aria-hidden="true"></span><h3>Live call</h3>' + extLine() + '</div>';
    h += startBtn();
    h += '<p class="hint">Start live call to have Tape listen to the Meet call. Beliefs, numbers, tie-downs and your next move update here as the prospect talks.</p>';
    var brief = prepBrief(CFG.state && CFG.state['ss-prep']);
    if (brief) h += '<div class="lv-card"><div class="k">Before the call</div><p>' + esc(brief).replace(/\n/g, '<br>') + '</p></div>';
    if (!st.pingTimer && !extReady()) h += installHelp();
    return h;
  }
  var lastHTML = '';
  function render() {
    var box = $('[data-live]'); if (!box) return;
    var inner;
    if (!st.session || st.session.status !== 'live') { st.moveShown = null; st.movePending = null; }
    if (st.session && st.session.status !== 'ended') inner = liveHTML();
    else if (st.justEnded) inner = endedHTML();
    else inner = idleHTML();
    if (st.error) inner += '<p class="lv-warn" role="alert">' + esc(st.error) + '</p>';
    var html = '<div class="lv">' + inner + '</div>';
    if (html !== lastHTML) { box.innerHTML = html; lastHTML = html; }
  }
  setInterval(function () {
    var t = $('[data-lvtimer]');
    if (t && st.session && st.session.status !== 'ended') t.textContent = mmss(since(st.session.firstAudioAt || st.session.startedAt));
  }, 1000);

  document.addEventListener('click', function (e) {
    var t = e.target && e.target.closest ? e.target : null;
    if (!t || !t.closest('[data-live]')) return;
    if (t.closest('[data-lvgolive]') || t.closest('[data-lvnew]')) { goLive(); return; }
    if (t.closest('[data-lvrearm]')) { rearm(); return; }
    if (t.closest('[data-lvconsent]')) { consent(); return; }
    if (t.closest('[data-lvend]')) { endCall(); return; }
    if (t.closest('[data-lvafter]')) { if (st.lastAnalysis) runAfterCall(st.lastAnalysis); return; }
    if (t.closest('[data-lvraised]')) { var tb = document.getElementById('tab-raised'); if (tb) tb.click(); return; }
    var sl = t.closest('[data-lvslide]');
    if (sl && window.SS && window.SS.deck) { window.SS.deck.open(Number(sl.dataset.lvslide)); return; }
    var go = t.closest('[data-lvgo]');
    if (go && window.SS && window.SS.goTo) { window.SS.goTo(go.dataset.lvgo); }
  });

  /* ---- Boot: wait for the script, then resume any open session ---- */
  function boot(tries) {
    if (!window.SS || !window.SS.applyCall || !$('[data-live]')) {
      if (tries > 100) return;
      setTimeout(function () { boot(tries + 1); }, 100);
      return;
    }
    ping();
    render();
    paintBeliefQuotes();
    api('/live/state?after=0&rev=0').then(function (j) {
      if (j.session && j.session.status !== 'ended') {
        st.session = j.session;
        (j.lines || []).forEach(function (l) { st.lines.push(l); });
        st.lastLineId = j.lastLineId || 0;
        if (j.coach) { st.rev = j.coach.rev; st.patch = j.coach.patch; applyPatch(st.patch); }
        startPolling();
      }
      render();
    }).catch(function () { render(); });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', function () { boot(0); }); else boot(0);
})();
