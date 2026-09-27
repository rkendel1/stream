// Stream desktop UI — presentation only.
//
// Every piece of data comes from the Stream AppPort surface via the desktop
// bridge. Nothing is stored here: no web storage, no browser database, no cache
// that could become authority. Reload and you see exactly what FeltDB holds.
'use strict';

// ---------------------------------------------------------------- transport

const token = new URLSearchParams(location.search).get('token');
const nativeIpc = window.ipc && typeof window.ipc.postMessage === 'function';
const pending = new Map();
let sequence = 0;

window.__streamReply = (response) => {
  const resolve = pending.get(response.id);
  if (resolve) {
    pending.delete(response.id);
    resolve(response);
  }
};

function call(capability, input = {}) {
  const request = { id: ++sequence, capability, input };
  const reply = nativeIpc
    ? new Promise((resolve) => {
        pending.set(request.id, resolve);
        window.ipc.postMessage(JSON.stringify(request));
      })
    : fetch('/ipc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-Stream-Token': token || '' },
        body: JSON.stringify(request),
      }).then((response) => response.json());
  return reply.then((response) => {
    if (!response.ok) {
      const error = new Error(response.error ? response.error.message : 'Stream request failed');
      error.code = response.error && response.error.code;
      throw error;
    }
    return response.result;
  });
}

// ---------------------------------------------------------------- helpers

function h(tag, props, ...children) {
  const element = document.createElement(tag);
  for (const [key, value] of Object.entries(props || {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') element.className = value;
    else if (key.startsWith('on')) element.addEventListener(key.slice(2), value);
    else if (value === true) element.setAttribute(key, '');
    else element.setAttribute(key, value);
  }
  for (const child of children.flat(Infinity)) {
    if (child === undefined || child === null || child === false) continue;
    element.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return element;
}

const $ = (selector) => document.querySelector(selector);

function host(url) {
  try {
    return new URL(url).host.replace(/^www\./, '');
  } catch {
    return url || '';
  }
}

function ago(timestamp) {
  if (!timestamp) return 'never';
  const seconds = Math.max(0, (Date.now() - new Date(timestamp).getTime()) / 1000);
  if (seconds < 60) return 'just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function plural(count, noun) {
  return `${count} ${noun}${count === 1 ? '' : 's'}`;
}

function externalLink(url, text) {
  return h('a', { href: url, target: '_blank', rel: 'noopener noreferrer' }, text || url);
}

let toastTimer;
function toast(message) {
  const element = $('#toast');
  element.textContent = message;
  element.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (element.hidden = true), 3500);
}

const KIND_LABEL = {
  web: 'Web page', rss: 'RSS feed', atom: 'Atom feed', json_feed: 'JSON Feed', github: 'GitHub',
  youtube: 'YouTube', documentation: 'Documentation', research: 'Research', api: 'API', webhook: 'Webhook',
};

// ---------------------------------------------------------------- state

const state = {
  tab: 'signals',
  signals: [],
  sources: [],
  targets: [],
  // Which targets' "Why am I watching this?" is open, and what it showed.
  // Presentation memory only; re-read from Stream on every refresh.
  expanded: new Map(),
  graph: null,
  contexts: [],
  jobs: new Map(),
  fresh: null,
  loaded: false,
  insights: [],
  // The conversation lives only here, in memory. It is an interface to
  // Stream, not a store: closing the window forgets it; Stream remembers
  // only what the user explicitly saves as an insight.
  chat: { turns: [], focus: null },
};

const TITLES = { signals: 'Today', chat: 'Ask Stream', sources: 'Sources', connections: 'Connections' };

async function refresh() {
  try {
    const [signals, sources, graph, contexts, insights, targets] = await Promise.all([
      call('stream.signal.list'),
      call('stream.source.list'),
      call('stream.connection.graph'),
      call('stream.context.list'),
      call('stream.insight.list'),
      call('stream.target.list'),
    ]);
    Object.assign(state, { signals, sources, graph, contexts, insights, targets, loaded: true });
    for (const id of state.expanded.keys()) {
      state.expanded.set(id, await call('stream.target.sources', { id }).catch(() => []));
    }
  } catch (error) {
    toast(`Could not reach Stream: ${error.message}`);
  }
  render();
}

// ---------------------------------------------------------------- add URL

const STAGES = [
  ['fetching', 'Fetching source'],
  ['understanding', 'Understanding content'],
  ['connecting', 'Finding connections'],
  ['building_signal', 'Building signal'],
];

// What the user typed is parsed by Stream, never here: the result says
// whether it is a resource or the information surface beneath a URL (/*).
async function addUrl(url) {
  let preview;
  try {
    preview = await call('stream.target.parse', { url });
  } catch (error) {
    toast(error.message);
    return;
  }
  if (preview.scope === 'descendants') return watchTarget(url, preview);
  let added;
  let source;
  try {
    // Durable first: the target and its source exist before anything is fetched.
    added = await call('stream.target.add', { url, observe: false });
    const surface = added.target.surfaces[0];
    source = (await call('stream.source.get', { id: surface.source_id })).source;
  } catch (error) {
    toast(error.message);
    return;
  }
  const job = { source, existing: added.existing, report: null, error: null };
  state.jobs.set(source.id, job);
  renderJobs();
  observe(job);
}

// A descendants target: durable intent → discovery → observation.
async function watchTarget(url, preview) {
  const job = { kind: 'target', target: null, preview, phase: 'adding', run: null, error: null };
  state.jobs.set(preview.id, job);
  renderJobs();
  try {
    const added = await call('stream.target.add', { url, observe: false });
    job.target = added.target;
    job.phase = 'discovering';
    renderJobs();
    const discovered = await call('stream.target.discover', { id: job.target.id });
    job.target = discovered.target;
    if (job.target.status === 'unavailable' || job.target.status === 'failed') {
      job.phase = job.target.status;
    } else {
      job.phase = 'observing';
      renderJobs();
      job.run = await call('stream.observation.run', { target_id: job.target.id, force: true });
      job.target = (await call('stream.target.get', { id: job.target.id })).target;
      job.phase = 'done';
    }
  } catch (error) {
    job.error = error.message;
    job.phase = 'failed';
  }
  renderJobs();
  await refresh();
}

async function observe(job) {
  job.report = null;
  job.error = null;
  job.source = { ...job.source, stage: 'queued' };
  renderJobs();
  // Progress is durable state: poll the source's processing stage.
  const poll = setInterval(async () => {
    try {
      const detail = await call('stream.source.get', { id: job.source.id });
      if (!job.report) {
        job.source = detail.source;
        renderJobs();
      }
    } catch { /* the next poll will tell */ }
  }, 300);
  try {
    job.report = await call('stream.source.observe', { id: job.source.id });
    job.source = job.report.source;
  } catch (error) {
    job.error = error.message;
  } finally {
    clearInterval(poll);
  }
  state.fresh = job.report && job.report.primary_signal_id;
  renderJobs();
  await refresh();
  if (state.fresh) {
    const card = document.querySelector(`[data-signal="${CSS.escape(state.fresh)}"]`);
    if (card) card.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }
}

function jobOutcome(job) {
  const report = job.report;
  if (job.error) return { failed: true, text: job.error };
  if (!report) return null;
  if (report.stage === 'failed') {
    return { failed: true, text: `${report.failure || 'Observation failed'}. The URL is saved.` };
  }
  if (report.signals_corroborated.length && report.primary_signal_id && report.signals_corroborated.includes(report.primary_signal_id)) {
    return { text: 'Recognized as the same change as an existing signal — added as corroborating evidence.' };
  }
  if (report.primary_signal_id || report.signals_created.length) {
    const extra = report.signals_created.length > 1 ? ` (${plural(report.signals_created.length, 'signal')} from this source)` : '';
    return { text: `Signal built${extra}.` };
  }
  return { text: report.source.stage_detail || 'Observed.' };
}

function surfaceList(target) {
  return h('ul', { class: 'surfaces' }, target.surfaces.map((surface) =>
    h('li', { class: `health-${surface.health}`, title: surface.title || surface.url },
      h('span', { class: 'mark' }, HEALTH_MARK[surface.health] || '•'), surface.label)));
}

const HEALTH_MARK = { healthy: '✓', pending: '…', retrying: '↻', unavailable: '✕', paused: '‖' };

function targetName(target) {
  return target.title || target.identity.display_name;
}

function targetJobCard(id, job) {
  const target = job.target;
  const name = target ? targetName(target) : job.preview.identity.display_name;
  const eyebrow = {
    adding: 'Adding…', discovering: 'Discovering…', observing: 'Observing…', done: 'Watching',
    unavailable: 'Added — observation unavailable', failed: 'Could not finish',
  }[job.phase];
  const surfaces = target && target.surfaces.length > 0;
  let result = null;
  if (job.phase === 'done' && job.run) {
    const signals = job.run.signals_created;
    result = signals ? `${plural(signals, 'signal')} built. Stream keeps watching and will surface what changes.`
      : 'Baseline recorded. Stream keeps watching and will surface what changes.';
  }
  if (job.phase === 'unavailable') result = (target.discovery_detail || '').replace(/^Observation unavailable:\s*/, '');
  if (job.phase === 'failed') result = job.error || (target && target.discovery_detail) || 'Discovery failed. The target is saved; Stream will try again.';
  return h('div', { class: `job target-job${job.phase === 'failed' ? ' failed' : ''}` },
    h('div', { class: 'job-head' },
      h('div', null,
        h('div', { class: 'eyebrow' }, eyebrow),
        h('div', { class: 'target-name' }, name),
        h('div', { class: 'job-url' }, target ? target.display_url : job.preview.display_url)),
      h('button', { class: 'subtle', type: 'button', 'aria-label': 'Dismiss', onclick: () => { state.jobs.delete(id); renderJobs(); } }, '✕')),
    h('p', { class: 'watching' }, target ? target.watching : 'Watching this information surface'),
    job.phase === 'discovering' || job.phase === 'adding' ? h('p', { class: 'muted small' }, 'Looking for feeds, the sitemap, and sections like blog, changelog, docs and releases…') : null,
    surfaces ? surfaceList(target) : null,
    result ? h('div', { class: 'job-result' }, h('span', null, result),
      job.phase === 'done' ? h('button', { class: 'link', type: 'button', onclick: () => { state.tab = 'sources'; render(); } }, 'Why these?') : null) : null);
}

function renderJobs() {
  const container = $('#jobs');
  container.replaceChildren();
  for (const [id, job] of state.jobs) {
    if (job.kind === 'target') {
      container.append(targetJobCard(id, job));
      continue;
    }
    const stage = job.source.stage;
    const index = STAGES.findIndex(([key]) => key === stage);
    const outcome = jobOutcome(job);
    const done = stage === 'observed' || (outcome && !outcome.failed);
    const title = job.source.title || host(job.source.original_url);
    const card = h('div', { class: `job${outcome && outcome.failed ? ' failed' : ''}` },
      h('div', { class: 'job-head' },
        h('div', null,
          h('div', { class: 'eyebrow' }, outcome ? (outcome.failed ? 'Could not finish' : 'Understood') : 'Analyzing…'),
          h('div', null, title),
          h('div', { class: 'job-url' }, job.source.original_url)),
        h('button', { class: 'subtle', type: 'button', 'aria-label': 'Dismiss', onclick: () => { state.jobs.delete(id); renderJobs(); } }, '✕')),
      outcome && outcome.failed ? null : h('ol', { class: 'stages' },
        STAGES.map(([key, label], position) => {
          const status = done || position < index ? 'done' : position === index ? 'active' : '';
          return h('li', { class: status }, h('span', { class: 'dot' }, status === 'done' ? '✓' : ''), label);
        })),
      outcome ? h('div', { class: 'job-result' },
        h('span', null, outcome.text),
        outcome.failed ? h('button', { class: 'ghost', type: 'button', onclick: () => observe(job) }, 'Retry') : null,
        job.report && job.report.primary_signal_id
          ? h('button', { class: 'link', type: 'button', onclick: () => openSignal(job.report.primary_signal_id) }, 'View signal')
          : null,
        job.report && job.report.primary_signal_id
          ? h('button', { class: 'ghost', type: 'button', onclick: () => askStream('Why does this matter to me?', { signal_ids: [job.report.primary_signal_id] }, job.source.title) }, 'Ask why this matters to me')
          : null) : null);
    container.append(card);
  }
}

// ---------------------------------------------------------------- views

function render() {
  for (const tab of document.querySelectorAll('[data-tab]')) {
    tab.setAttribute('aria-selected', String(tab.dataset.tab === state.tab));
  }
  $('#view-title').textContent = TITLES[state.tab];
  const view = $('#view');
  view.replaceChildren();
  if (!state.loaded) {
    view.append(h('p', { class: 'muted' }, 'Loading…'));
    return;
  }
  if (state.tab === 'signals') view.append(...renderSignals());
  if (state.tab === 'chat') view.append(...renderChat());
  if (state.tab === 'sources') view.append(...renderSources());
  if (state.tab === 'connections') view.append(...renderConnections());
}

function chip(label) {
  const classes = ['chip', label.kind === 'project' ? 'project' : '', label.relation === 'via' ? 'via' : ''].join(' ');
  return h('span', { class: classes, title: label.relation === 'via' ? 'Connected indirectly, through a related context' : `${label.kind} · ${label.relation}` }, label.label);
}

function emptyState(title, text, actions) {
  return h('div', { class: 'empty' }, h('h2', null, title), h('p', null, text), h('div', { class: 'row' }, actions));
}

function renderSignals() {
  if (!state.signals.length) {
    return [emptyState(
      'Give Stream something you want to know about',
      state.contexts.length
        ? 'Paste a URL. Stream will work out what changed and how it connects to what you care about.'
        : 'Start by telling Stream what you care about, then paste a URL.',
      [
        state.contexts.length ? null : h('button', { class: 'ghost', type: 'button', onclick: openContextForm }, '+ Context'),
        h('button', { class: 'primary', type: 'button', onclick: openAddForm }, '+ Add URL'),
      ])];
  }
  return state.signals.map(signalCard);
}

function signalCard(summary) {
  const signal = summary.signal;
  const primary = summary.primary;
  const origin = primary ? `from “${primary.title}” · ${host(primary.url || (primary.source && primary.source.canonical_url))}` : '';
  return h('article', { class: `signal${state.fresh === signal.id ? ' fresh' : ''}`, 'data-signal': signal.id },
    h('div', { class: 'signal-top' },
      h('span', { class: 'eyebrow' }, signal.topic.label),
      h('span', { class: 'rank', title: summary.ranking.summary }, `#${summary.position}`)),
    h('h2', { class: 'subject' }, signal.subject.label),
    h('p', { class: 'change' }, signal.change.statement),
    signal.why_it_matters
      ? h('div', { class: 'why' }, h('p', { class: 'section-label' }, 'Why it matters'), h('p', null, signal.why_it_matters))
      : h('div', { class: 'why unconnected' }, h('p', null, 'Not connected to anything you’ve told Stream you care about yet.')),
    summary.connected_to.length
      ? h('div', { class: 'connected' }, h('p', { class: 'section-label' }, 'Connected to'), h('div', { class: 'chips' }, summary.connected_to.map(chip)))
      : null,
    h('div', { class: 'signal-foot' },
      h('button', { class: 'link', type: 'button', onclick: () => openSignal(signal.id, 'evidence') },
        `${plural(summary.source_count, 'source')} · View evidence`),
      h('button', { class: 'link', type: 'button', onclick: () => openSignal(signal.id, 'ranking') }, 'Why here?'),
      askMenu({ signal_ids: [signal.id] }, signal.subject.label),
      h('span', { class: 'spacer' }),
      h('button', { class: 'subtle', type: 'button', title: 'Resolved — remove from Today', onclick: () => setStatus(signal.id, 'resolve') }, 'Done'),
      h('button', { class: 'subtle', type: 'button', title: 'Not useful — remove from Today', onclick: () => setStatus(signal.id, 'dismiss') }, 'Dismiss')),
    origin ? h('div', { class: 'origin' }, origin) : null);
}

async function setStatus(id, action) {
  try {
    await call(`stream.signal.${action}`, { id });
    toast(action === 'resolve' ? 'Marked done. It stays in Stream’s memory.' : 'Dismissed. It stays in Stream’s memory.');
    await refresh();
  } catch (error) {
    toast(error.message);
  }
}

function renderSources() {
  if (!state.sources.length && !state.targets.length) {
    return [emptyState('Nothing watched yet', 'Add a URL to watch it, or end it in /* to watch the information surface beneath it.', [
      h('button', { class: 'primary', type: 'button', onclick: openAddForm }, '+ Add URL'),
    ])];
  }
  const inTarget = new Set(state.targets.flatMap((t) => t.surfaces.map((s) => s.source_id)));
  const nodes = [];
  const surfaces = state.targets.filter((t) => t.scope === 'descendants');
  if (surfaces.length) {
    nodes.push(h('div', { class: 'group-title' }, 'Information surfaces'));
    nodes.push(...surfaces.map(targetCard));
  }
  const resources = state.sources.filter((source) => !inTarget.has(source.id) || state.targets.some((t) => t.scope === 'resource' && t.surfaces.some((s) => s.source_id === source.id)));
  if (resources.length) {
    if (surfaces.length) nodes.push(h('div', { class: 'group-title' }, 'Resources'));
    nodes.push(...resources.map(sourceRow));
  }
  return nodes;
}

function targetCard(target) {
  const expanded = state.expanded.has(target.id);
  const paused = target.status === 'paused';
  const why = h('details', { class: 'why-watch', open: expanded, ontoggle: async (event) => {
    const open = event.currentTarget.open;
    if (open && !state.expanded.has(target.id)) {
      state.expanded.set(target.id, null);
      try {
        state.expanded.set(target.id, await call('stream.target.sources', { id: target.id }));
      } catch (error) {
        toast(error.message);
      }
      render();
    } else if (!open) {
      state.expanded.delete(target.id);
    }
  } },
    h('summary', null, 'Why am I watching this?'),
    expanded && state.expanded.get(target.id) ? watchedList(state.expanded.get(target.id)) : h('p', { class: 'muted small' }, 'Loading…'));
  const act = (capability, message) => async () => {
    try {
      if (capability === 'observe') {
        toast('Observing now…');
        const run = await call('stream.observation.run', { target_id: target.id, force: true });
        toast(runSummary(run));
      } else {
        const result = await call(capability, { id: target.id });
        if (message) toast(typeof message === 'function' ? message(result) : message);
      }
    } catch (error) {
      toast(error.message);
    }
    await refresh();
  };
  return h('div', { class: `target status-${target.status}`, 'data-target': target.id },
    h('div', { class: 'target-head' },
      h('div', null,
        h('div', { class: 'target-name' }, targetName(target)),
        h('div', { class: 'job-url' }, target.display_url)),
      h('span', { class: `stage ${target.status}` }, target.status)),
    h('p', { class: 'watching' }, target.watching),
    target.status === 'unavailable' || target.status === 'failed'
      ? h('p', { class: 'muted small' }, (target.discovery_detail || '').replace(/^Observation unavailable:\s*/, '')) : null,
    target.surfaces.length ? surfaceList(target) : null,
    h('div', { class: 'source-meta' },
      `Discovered ${ago(target.last_discovered_at)} · next discovery ${until(target.next_discovery_at)} · last observed ${ago(target.last_observed_at)}`),
    h('div', { class: 'target-actions' },
      h('button', { class: 'subtle', type: 'button', onclick: act('stream.target.discover', (r) => r.new_sources.length ? `Found ${plural(r.new_sources.length, 'new surface')}.` : 'No new surfaces.') }, 'Discover now'),
      h('button', { class: 'subtle', type: 'button', onclick: act('observe') }, 'Observe now'),
      h('button', { class: 'subtle', type: 'button', onclick: act(paused ? 'stream.target.resume' : 'stream.target.pause', paused ? 'Watching again.' : 'Paused. Nothing is forgotten.') }, paused ? 'Resume' : 'Pause')),
    why);
}

function runSummary(run) {
  const parts = [`Observed ${plural(run.sources_observed, 'surface')}`];
  if (run.signals_created) parts.push(`${plural(run.signals_created, 'new signal')}`);
  if (run.signals_corroborated) parts.push(`${run.signals_corroborated} corroborated`);
  if (!run.signals_created && !run.signals_corroborated) parts.push(run.new_items || run.page_changes ? 'nothing that needs your attention' : 'nothing new');
  if (run.sources_failed) parts.push(`${plural(run.sources_failed, 'problem')}`);
  return parts.join(' · ');
}

function until(timestamp) {
  if (!timestamp) return 'not scheduled';
  const seconds = (new Date(timestamp).getTime() - Date.now()) / 1000;
  if (seconds <= 60) return 'now';
  if (seconds < 3600) return `in ${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `in ${Math.round(seconds / 3600)}h`;
  return `in ${Math.round(seconds / 86400)}d`;
}

function watchedList(watched) {
  return h('ul', { class: 'watched' }, watched.map((w) => {
    const source = w.source;
    return h('li', { class: `health-${w.health}` },
      h('div', { class: 'watched-head' },
        h('span', { class: 'mark' }, HEALTH_MARK[w.health] || '•'),
        h('strong', null, SURFACE_LABEL(source.surface_kind)),
        h('span', null, ' '),
        externalLink(source.canonical_url, source.title || source.canonical_url)),
      h('p', { class: 'small' }, w.why),
      h('p', { class: 'muted small' },
        `last observed ${ago(source.last_observed_at)}`,
        w.health === 'retrying' || w.health === 'unavailable' ? ` · ${source.last_error_message || 'failing'}` : ''),
      w.last_change && w.last_change.change !== 'baseline' ? h('p', { class: 'small change-note' }, `Last change: ${w.last_change.summary}`) : null);
  }));
}

function SURFACE_LABEL(kind) {
  return kind ? kind.charAt(0).toUpperCase() + kind.slice(1) : 'Page';
}

function sourceRow(source) {
    const observedVia = source.adapter_kind !== source.kind && source.adapter_kind !== 'web'
      ? ` · observed via ${KIND_LABEL[source.adapter_kind] || source.adapter_kind}` : '';
    const job = { source, existing: true, report: null, error: null };
    return h('div', { class: 'source' },
      h('div', null,
        h('div', { class: 'source-title' }, source.title || host(source.canonical_url)),
        h('div', { class: 'source-url' }, externalLink(source.original_url))),
      h('span', { class: `stage ${source.stage}` }, source.stage.replace('_', ' ')),
      h('div', { class: 'source-meta' },
        `${KIND_LABEL[source.kind] || source.kind}${observedVia} · Watching this resource · added ${ago(source.discovered_at)} · last observed ${ago(source.last_observed_at)}`,
        source.stage_detail && source.stage !== 'failed' ? ` · ${source.stage_detail}` : ''),
      source.stage === 'failed'
        ? h('div', { class: 'source-error' }, h('span', null, source.stage_detail || source.last_error_message || 'Observation failed'),
            h('button', { class: 'ghost', type: 'button', onclick: () => { state.jobs.set(source.id, job); observe(job); } }, 'Retry'))
        : h('div', null,
            h('button', { class: 'subtle', type: 'button', onclick: () => askStream('Why does this matter to me?', { source_ids: [source.id] }, source.title || host(source.canonical_url)) }, 'Ask why this matters'),
            h('button', { class: 'subtle', type: 'button', onclick: () => { state.jobs.set(source.id, job); observe(job); } }, 'Observe now')));
}

function renderConnections() {
  const graph = state.graph || { contexts: [], subjects: [] };
  const nodes = [];
  nodes.push(h('div', { class: 'group-title' }, 'What you care about'));
  if (!graph.contexts.length) {
    nodes.push(emptyState('Tell Stream what matters to you',
      'Context is how Stream decides why something matters: projects, interests, concerns.',
      [h('button', { class: 'primary', type: 'button', onclick: openContextForm }, '+ Context')]));
  }
  for (const node of graph.contexts) {
    const direct = node.signals.filter((s) => s.relation !== 'via');
    const indirect = node.signals.filter((s) => s.relation === 'via');
    nodes.push(h('div', { class: 'node' },
      h('h3', null, node.context.name, h('span', { class: 'kind' }, node.context.kind),
        h('span', { class: 'spacer' }),
        h('button', { class: 'subtle', type: 'button', onclick: () => askStream(`What have we learned about ${node.context.name}?`, { context_ids: [node.context.id] }, node.context.name) }, 'Ask what we’ve learned')),
      node.context.description ? h('p', { class: 'desc' }, node.context.description) : null,
      node.related.length ? h('div', { class: 'chips' }, node.related.map((r) => h('span', { class: 'chip via' }, `↔ ${r.label}`))) : null,
      node.signals.length
        ? h('ul', { class: 'tree' },
            direct.map((s) => h('li', null, h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.subject), ` — ${s.change}`)),
            indirect.map((s) => h('li', { class: 'indirect' }, h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.subject), ` — ${s.change} (through a related context)`)))
        : h('p', { class: 'muted small' }, 'Nothing observed about this yet.')));
  }
  if (state.insights.length) {
    nodes.push(h('div', { class: 'group-title' }, 'Saved reasoning'));
    nodes.push(...state.insights.map(insightCard));
  }
  const converging = graph.subjects.filter((s) => s.observation_count > 1);
  if (converging.length) {
    nodes.push(h('div', { class: 'group-title' }, 'Converging observations'));
    for (const subject of converging) {
      nodes.push(h('div', { class: 'node' },
        h('h3', null, subject.label, h('span', { class: 'kind' }, `${subject.observation_count} observations`)),
        h('ul', { class: 'tree' }, subject.signals.map((s) =>
          h('li', null, h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.change))))));
    }
  }
  return nodes;
}

// ---------------------------------------------------------------- signal drawer

const CLAIM_ORDER = ['why_it_matters', 'connection', 'change', 'subject', 'topic', 'corroboration'];
const CLAIM_LABEL = {
  why_it_matters: 'Why it matters', connection: 'Connection to your context', change: 'What changed',
  subject: 'Subject', topic: 'Topic', corroboration: 'Corroborating observation',
};

function sameDocument(a, b) {
  try {
    const left = new URL(a);
    const right = new URL(b);
    return left.host.replace(/^www\./, '') === right.host.replace(/^www\./, '') && left.pathname.replace(/\/$/, '') === right.pathname.replace(/\/$/, '');
  } catch {
    return false;
  }
}

// One excerpt, the claims it supports, and its chain back to the URL:
// excerpt → item → source → URL.
function evidenceGroup(group) {
  const { trace, claims } = group;
  const evidence = trace.evidence;
  const item = trace.item;
  const source = trace.source;
  let via = evidence.source_id;
  if (source) {
    const name = source.title || host(source.canonical_url);
    via = item && item.url && !sameDocument(item.url, source.canonical_url) && source.adapter_kind !== 'web'
      ? `found in the ${KIND_LABEL[source.adapter_kind] || source.adapter_kind} of “${name}”`
      : `${KIND_LABEL[source.kind] || source.kind} · ${name}`;
  }
  let path = evidence.url;
  try { path = host(evidence.url) + new URL(evidence.url).pathname; } catch { /* keep raw */ }
  return h('div', { class: 'claim' },
    h('div', { class: 'claim-label' }, claims.map((claim) => CLAIM_LABEL[claim]).join(' · ')),
    h('blockquote', null, evidence.excerpt),
    h('div', { class: 'trace' },
      h('span', null, `in the ${evidence.locator} of “${item ? item.title : evidence.item_id}”`),
      h('span', { class: 'arrow' }, '→'),
      h('span', null, via),
      h('span', { class: 'arrow' }, '→'),
      externalLink(evidence.url, path)));
}

function groupEvidence(traces) {
  const groups = new Map();
  for (const trace of traces) {
    const e = trace.evidence;
    const key = `${e.item_id}\u0000${e.locator}\u0000${e.excerpt}`;
    const group = groups.get(key) || { trace, claims: [] };
    if (!group.claims.includes(e.claim)) group.claims.push(e.claim);
    groups.set(key, group);
  }
  const rank = (claim) => CLAIM_ORDER.indexOf(claim);
  for (const group of groups.values()) group.claims.sort((a, b) => rank(a) - rank(b));
  return [...groups.values()].sort((a, b) => rank(a.claims[0]) - rank(b.claims[0]));
}

// Set through the CSSOM: the CSP forbids inline style attributes.
function barFill(fraction) {
  const fill = h('span');
  fill.style.width = `${Math.round(Math.max(0, Math.min(1, fraction)) * 100)}%`;
  return fill;
}

function closeDrawer() {
  $('#drawer').hidden = true;
  $('#scrim').hidden = true;
}

async function openSignal(id, focus) {
  let detail;
  try {
    detail = await call('stream.signal.get', { id });
  } catch (error) {
    toast(error.message);
    return;
  }
  const summary = detail.summary;
  const signal = summary.signal;
  const groups = groupEvidence(detail.evidence);
  const maxWeight = Math.max(...summary.ranking.factors.map((f) => f.weight));

  const drawer = $('#drawer');
  drawer.replaceChildren(
    h('div', { class: 'drawer-head' },
      h('div', null,
        h('div', { class: 'eyebrow' }, signal.topic.label),
        h('h2', null, signal.subject.label),
        h('p', { class: 'change' }, signal.change.statement)),
      h('button', { class: 'subtle', type: 'button', 'aria-label': 'Close', onclick: closeDrawer }, '✕')),
    h('div', { class: 'drawer-ask' }, askMenu({ signal_ids: [signal.id] }, signal.subject.label, true)),
    h('section', null,
      h('h3', null, 'Why does Stream think this matters?'),
      h('p', null, signal.why_it_matters || 'It isn’t connected to anything you’ve told Stream you care about yet. Add context and Stream will re-evaluate it.'),
      signal.why_it_matters ? h('p', { class: 'muted small' }, 'This is Stream’s inference from your context, not something a source states.') : null,
      summary.connected_to.length ? h('div', { class: 'chips' }, summary.connected_to.map(chip)) : null),
    detail.claims.length ? h('section', null,
      h('h3', null, 'What Stream observed, and what it infers'),
      detail.claims.map((claim) => h('div', { class: `claim-row basis-${claim.basis}` },
        basisBadge(claim.basis),
        h('span', null, claim.statement),
        claim.evidence_ids.length ? h('span', { class: 'muted small' }, ` · ${plural(claim.evidence_ids.length, 'excerpt')}`) : null))) : null,
    detail.synthesis ? h('section', null,
      h('h3', null, `Across ${plural(detail.synthesis.observation_count, 'observation')} from ${plural(detail.synthesis.source_count, 'source')}`),
      [['Agree', detail.synthesis.agreements], ['New', detail.synthesis.new_information], ['Differ', detail.synthesis.differences], ['Uncertain', detail.synthesis.uncertainties]]
        .filter(([, points]) => points.length)
        .map(([label, points]) => h('div', { class: 'synthesis-group' },
          h('div', { class: 'claim-label' }, label),
          points.map((point) => h('div', { class: `claim-row basis-${point.basis}` }, basisBadge(point.basis), h('span', null, point.statement)))))) : null,
    detail.insights.length ? h('section', null, h('h3', null, 'Your saved reasoning'), detail.insights.map(insightCard)) : null,
    h('section', { id: 'drawer-evidence' },
      h('h3', null, `Evidence · ${plural(groups.length, 'excerpt')} from ${plural(detail.observations.length, 'observation')}`),
      groups.map(evidenceGroup)),
    h('section', { id: 'drawer-ranking' },
      h('h3', null, summary.position ? `Why is this #${summary.position} today?` : 'Why is this not in Today?'),
      h('p', { class: 'muted small' }, summary.ranking.summary),
      summary.ranking.factors.map((factor) =>
        h('div', { class: 'factor' },
          h('span', null, factor.label),
          h('span', { class: 'value' }, factor.contribution.toFixed(2)),
          h('div', { class: 'meter' }, barFill(factor.contribution / maxWeight)),
          h('span', { class: 'explain' }, factor.explanation)))),
    h('section', null,
      h('h3', null, 'Observations'),
      detail.observations.map((item) =>
        h('div', { class: 'observation' },
          h('div', null, item.url ? externalLink(item.url, item.title) : item.title),
          h('div', { class: 'muted small' },
            `${item.source ? (KIND_LABEL[item.source.kind] || item.source.kind) + ' · ' + host(item.source.canonical_url) : ''} · observed ${ago(item.observed_at)}`)))),
    h('p', { class: 'advisory-note' },
      'Stream’s interpretation is advisory. Every claim above quotes the source material it came from, and the sources themselves remain the record.'));
  drawer.hidden = false;
  $('#scrim').hidden = false;
  drawer.scrollTop = 0;
  const target = focus && document.getElementById(`drawer-${focus}`);
  if (target) target.scrollIntoView({ block: 'start' });
  drawer.focus();
}

// ---------------------------------------------------------------- chat

const BASIS_LABEL = { observed: 'Observed', connected: 'Connected', inferred: 'Inferred', hypothesis: 'Hypothesis' };
const BASIS_HINT = {
  observed: 'Stated by a source — see the quoted evidence',
  connected: 'A relationship to your context or to other signals, backed by evidence',
  inferred: 'Stream’s conclusion from the evidence — not stated by a source',
  hypothesis: 'A possibility worth investigating — not established',
};
const ASK_PRESETS = [
  'Why does this matter?',
  'What changed?',
  'What does this connect to?',
  'What else supports this?',
  'What contradicts this?',
  'What should we investigate?',
];

function basisBadge(basis) {
  return h('span', { class: `basis basis-${basis}`, title: BASIS_HINT[basis] }, BASIS_LABEL[basis] || basis);
}

// "Ask Stream" from anything: preset questions, retrieval constrained to it.
function askMenu(focus, label, open) {
  return h('details', { class: 'ask-menu', open: open || false },
    h('summary', null, 'Ask Stream'),
    h('div', { class: 'ask-options' },
      ASK_PRESETS.map((question) => h('button', { class: 'chip toggle', type: 'button', onclick: (event) => {
        event.currentTarget.closest('details').open = false;
        askStream(question, focus, label);
      } }, question))));
}

function askStream(question, focus, label) {
  closeDrawer();
  state.chat.focus = focus ? { focus, label } : state.chat.focus;
  state.tab = 'chat';
  send(question);
}

async function send(question) {
  question = question.trim();
  if (!question) return;
  const history = state.chat.turns
    .filter((turn) => turn.answer)
    .slice(-3)
    .map((turn) => ({ question: turn.question, signal_ids: turn.answer.signals.map((s) => s.id) }));
  const turn = { question, focus: state.chat.focus, answer: null, error: null };
  state.chat.turns.push(turn);
  render();
  try {
    turn.answer = await call('stream.chat.ask', { question, focus: turn.focus ? turn.focus.focus : {}, history });
  } catch (error) {
    turn.error = error.message;
  }
  render();
  const answers = document.querySelectorAll('.turn');
  if (answers.length) answers[answers.length - 1].scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function suggestions() {
  const out = [];
  for (const view of state.contexts.slice(0, 2)) out.push(`What have we learned about ${view.context.name}?`);
  if (state.contexts.some((v) => v.context.kind === 'project')) out.push('Which recent developments connect to the things I’m building?');
  out.push('What changed recently?', 'What patterns are appearing across these sources?', 'What remains uncertain?', 'What should I investigate further?');
  return out;
}

function renderChat() {
  const nodes = [];
  const focus = state.chat.focus;
  const form = h('form', { class: 'ask-form', onsubmit: (event) => {
    event.preventDefault();
    const input = event.currentTarget.querySelector('input');
    const question = input.value;
    input.value = '';
    send(question);
  } },
    h('input', { type: 'text', name: 'question', placeholder: focus ? `Ask about ${focus.label}…` : 'Ask what Stream knows…', autocomplete: 'off', 'aria-label': 'Question' }),
    h('button', { class: 'primary', type: 'submit' }, 'Ask'));
  nodes.push(form);
  nodes.push(h('div', { class: 'ask-meta' },
    focus
      ? h('span', { class: 'chip scope' }, `Scope: ${focus.label}`, h('button', { class: 'chip-x', type: 'button', 'aria-label': 'Ask about everything', onclick: () => { state.chat.focus = null; render(); } }, '✕'))
      : h('span', { class: 'muted small' }, 'Scope: everything Stream knows'),
    state.chat.turns.length ? h('button', { class: 'subtle', type: 'button', onclick: () => { state.chat = { turns: [], focus: null }; render(); } }, 'New conversation') : null));

  if (!state.chat.turns.length) {
    nodes.push(h('div', { class: 'chat-intro' },
      h('p', null, 'Ask what Stream knows. Answers come from your sources and context, cite the evidence they rest on, and say plainly when Stream doesn’t know.'),
      h('div', { class: 'chips' }, suggestions().map((q) => h('button', { class: 'chip toggle', type: 'button', onclick: () => send(q) }, q)))));
  }
  for (const turn of state.chat.turns) {
    nodes.push(h('div', { class: 'turn' },
      h('div', { class: 'question' }, turn.question, turn.focus ? h('span', { class: 'muted small' }, ` — about ${turn.focus.label}`) : null),
      turn.error ? h('div', { class: 'answer failed' }, turn.error)
        : turn.answer ? answerCard(turn.answer)
        : h('div', { class: 'answer pending' }, 'Stream is looking through what it knows…')));
  }
  return nodes;
}

function evidenceList(traces) {
  return h('div', { class: 'evidence-list' }, traces.map((trace) => evidenceGroup({ trace, claims: [trace.evidence.claim] })));
}

function answerCard(answer) {
  const traceById = new Map(answer.evidence.map((trace) => [trace.evidence.id, trace]));
  const card = h('div', { class: `answer sufficiency-${answer.sufficiency}` });
  card.append(h('p', { class: 'answer-summary' }, answer.summary));
  if (answer.sufficiency === 'partial') {
    card.append(h('p', { class: 'answer-banner' }, 'Stream’s evidence only partly answers this.'));
  }
  if (answer.statements.length) {
    card.append(h('ol', { class: 'statements' }, answer.statements.map((statement) => {
      const traces = statement.evidence_ids.map((id) => traceById.get(id)).filter(Boolean);
      const row = h('li', { class: `statement basis-${statement.basis}` },
        basisBadge(statement.basis),
        h('span', { class: 'statement-text' }, statement.text));
      if (traces.length) {
        const details = h('details', { class: 'statement-evidence' },
          h('summary', null, `${plural(new Set(traces.map((t) => t.source && t.source.id)).size, 'source')} · ${plural(traces.length, 'excerpt')}`),
          evidenceList(traces));
        row.append(details);
      }
      return row;
    })));
  }
  if (answer.uncertainties.length) {
    card.append(h('div', { class: 'answer-section' },
      h('div', { class: 'claim-label' }, 'Uncertain'),
      h('ul', { class: 'uncertain' }, answer.uncertainties.map((u) => h('li', null, u)))));
  }
  if (answer.connected_to.length) {
    card.append(h('div', { class: 'answer-section' },
      h('div', { class: 'claim-label' }, 'Connected to'),
      h('div', { class: 'chips' }, answer.connected_to.map(chip))));
  }
  const sources = h('div', { class: 'answer-sources', hidden: true }, evidenceList(answer.evidence));
  const saveSlot = h('div');
  card.append(h('div', { class: 'answer-foot' },
    answer.evidence.length
      ? h('button', { class: 'link', type: 'button', onclick: () => (sources.hidden = !sources.hidden) },
          `Evidence · ${plural(answer.sources.length, 'source')} · View sources`)
      : h('span', { class: 'muted small' }, 'No evidence cited'),
    answer.signals.map((s) => h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.subject)),
    h('span', { class: 'spacer' }),
    answer.statements.length ? h('button', { class: 'ghost', type: 'button', onclick: () => {
      saveSlot.replaceChildren(saveInsightForm(answer, () => saveSlot.replaceChildren()));
    } }, 'Save insight') : null));
  card.append(sources, saveSlot);
  if (answer.follow_ups.length) {
    card.append(h('div', { class: 'chips follow-ups' }, answer.follow_ups.map((q) => h('button', { class: 'chip toggle', type: 'button', onclick: () => send(q) }, q))));
  }
  card.append(h('p', { class: 'muted small retrieved' },
    `Stream looked at ${plural(answer.retrieved.signal_ids.length, 'signal')} and ${plural(answer.retrieved.evidence_count, 'evidence excerpt')}${answer.retrieved.focused ? ' you pointed at' : ''}. Nothing else informs this answer.`));
  for (const notice of answer.notices) card.append(h('p', { class: 'notice' }, notice));
  return card;
}

function saveInsightForm(answer, done) {
  const preferred = answer.statements.find((s) => s.basis === 'inferred') || answer.statements[0];
  const cited = [...new Set(answer.statements.flatMap((s) => s.evidence_ids))];
  const form = h('form', { class: 'save-insight', onsubmit: async (event) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    try {
      const kind = data.get('kind');
      await call('stream.insight.save', {
        kind,
        statement: data.get('statement'),
        question: answer.question,
        evidence_ids: cited,
        context_ids: preferred.context_ids,
        uncertainty: answer.uncertainties[0] || null,
      });
      toast('Saved to Stream as advisory knowledge, with its evidence.');
      done();
      refresh();
    } catch (error) {
      toast(error.message);
    }
  } },
    h('label', { class: 'claim-label' }, 'Save to Stream'),
    h('textarea', { name: 'statement', rows: '3', required: true }, preferred.text),
    h('div', { class: 'composer-row' },
      h('select', { name: 'kind', 'aria-label': 'Kind' },
        [['insight', 'Insight'], ['hypothesis', 'Hypothesis'], ['question', 'Open question'], ['investigation', 'Investigation'], ['decision_candidate', 'Decision candidate']]
          .map(([value, label]) => h('option', { value }, label))),
      h('span', { class: 'muted small' }, `cites ${plural(cited.length, 'excerpt')}`),
      h('span', { class: 'spacer' }),
      h('button', { class: 'ghost', type: 'button', onclick: done }, 'Cancel'),
      h('button', { class: 'primary', type: 'submit' }, 'Save')));
  return form;
}

const INSIGHT_LABEL = { insight: 'Insight', hypothesis: 'Hypothesis', question: 'Open question', investigation: 'Investigation', decision_candidate: 'Decision candidate' };

function insightCard(insight) {
  return h('div', { class: `node insight status-${insight.status}` },
    h('div', { class: 'insight-head' },
      h('span', { class: 'eyebrow' }, INSIGHT_LABEL[insight.kind] || insight.kind),
      basisBadge(insight.basis),
      insight.status !== 'open' ? h('span', { class: 'muted small' }, insight.status) : null,
      h('span', { class: 'spacer' }),
      insight.status === 'open' ? h('button', { class: 'subtle', type: 'button', onclick: async () => {
        await call('stream.insight.resolve', { id: insight.id });
        refresh();
      } }, 'Resolve') : null),
    h('p', null, insight.statement),
    insight.question ? h('p', { class: 'muted small' }, `From: “${insight.question}”`) : null,
    h('p', { class: 'muted small' }, `${plural(insight.evidence_ids.length, 'excerpt')} of evidence · saved ${ago(insight.created_at)} · advisory`));
}

// ---------------------------------------------------------------- forms

function openAddForm() {
  $('#context-form').hidden = true;
  $('#add-form').hidden = false;
  $('#add-url').focus();
}

function openContextForm() {
  $('#add-form').hidden = true;
  const form = $('#context-form');
  const related = $('#context-related');
  related.replaceChildren(...state.contexts.map((view) =>
    h('button', {
      class: 'chip toggle', type: 'button', 'aria-pressed': 'false', 'data-id': view.context.id,
      onclick: (event) => {
        const button = event.currentTarget;
        button.setAttribute('aria-pressed', String(button.getAttribute('aria-pressed') !== 'true'));
      },
    }, view.context.name)));
  form.querySelector('.related-row').hidden = state.contexts.length === 0;
  form.hidden = false;
  $('#context-name').focus();
}

function wireForms() {
  $('#open-add').addEventListener('click', openAddForm);
  $('#open-context').addEventListener('click', openContextForm);
  for (const button of document.querySelectorAll('[data-close]')) {
    button.addEventListener('click', () => (button.closest('form').hidden = true));
  }
  let previewTimer;
  $('#add-url').addEventListener('input', (event) => {
    clearTimeout(previewTimer);
    const value = event.currentTarget.value.trim();
    const preview = $('#add-preview');
    if (!value) {
      preview.hidden = true;
      return;
    }
    previewTimer = setTimeout(async () => {
      try {
        const parsed = await call('stream.target.parse', { url: value });
        preview.className = `preview scope-${parsed.scope}`;
        preview.replaceChildren(
          h('strong', null, parsed.scope === 'descendants' ? 'Observation target' : 'Resource'),
          ` · ${parsed.identity.display_name} · `,
          parsed.scope === 'descendants' ? parsed.identity.watching : 'Watching this resource',
          h('span', { class: 'muted' }, ` — ${parsed.display_url}`));
      } catch (error) {
        preview.className = 'preview invalid';
        preview.textContent = error.message.replace(/^.*?: /, '');
      }
      preview.hidden = false;
    }, 200);
  });
  $('#add-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const input = $('#add-url');
    const url = input.value.trim();
    if (!url) return;
    input.value = '';
    $('#add-preview').hidden = true;
    $('#add-form').hidden = true;
    addUrl(url);
  });
  $('#context-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    const related = [...form.querySelectorAll('.chip.toggle[aria-pressed=true]')].map((b) => b.dataset.id);
    try {
      const context = await call('stream.context.add', {
        name: data.get('name'),
        kind: data.get('kind'),
        description: data.get('description') || null,
        related,
      });
      form.reset();
      form.hidden = true;
      toast(`Stream will now evaluate everything against “${context.name}”.`);
      await refresh();
    } catch (error) {
      toast(error.message);
    }
  });
  for (const tab of document.querySelectorAll('[data-tab]')) {
    tab.addEventListener('click', () => {
      state.tab = tab.dataset.tab;
      render();
    });
  }
  $('#scrim').addEventListener('click', closeDrawer);
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeDrawer();
      for (const form of document.querySelectorAll('form')) form.hidden = true;
    }
  });
  window.addEventListener('focus', () => { if (!state.jobs.size) refresh(); });
  // Stream keeps observing in the background; show what it found.
  setInterval(() => {
    const busy = [...state.jobs.values()].some((job) => job.kind === 'target' ? !['done', 'failed', 'unavailable'].includes(job.phase) : !job.report && !job.error);
    if (!busy && state.tab !== 'chat' && $('#drawer').hidden && document.visibilityState === 'visible') refresh();
  }, 30000);
}

wireForms();
render();
refresh();
