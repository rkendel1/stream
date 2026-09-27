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
  graph: null,
  contexts: [],
  jobs: new Map(),
  fresh: null,
  loaded: false,
};

async function refresh() {
  try {
    const [signals, sources, graph, contexts] = await Promise.all([
      call('stream.signal.list'),
      call('stream.source.list'),
      call('stream.connection.graph'),
      call('stream.context.list'),
    ]);
    Object.assign(state, { signals, sources, graph, contexts, loaded: true });
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

async function addUrl(url) {
  let added;
  try {
    // Durable first: the source exists before anything is fetched.
    added = await call('stream.source.add', { url, observe: false });
  } catch (error) {
    toast(error.message);
    return;
  }
  const job = { source: added.source, existing: added.existing, report: null, error: null };
  state.jobs.set(added.source.id, job);
  renderJobs();
  observe(job);
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

function renderJobs() {
  const container = $('#jobs');
  container.replaceChildren();
  for (const [id, job] of state.jobs) {
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
          : null) : null);
    container.append(card);
  }
}

// ---------------------------------------------------------------- views

function render() {
  for (const tab of document.querySelectorAll('[data-tab]')) {
    tab.setAttribute('aria-selected', String(tab.dataset.tab === state.tab));
  }
  const view = $('#view');
  view.replaceChildren();
  if (!state.loaded) {
    view.append(h('p', { class: 'muted' }, 'Loading…'));
    return;
  }
  if (state.tab === 'signals') view.append(...renderSignals());
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
  if (!state.sources.length) {
    return [emptyState('No sources yet', 'Every URL you add becomes a durable source Stream keeps observing.', [
      h('button', { class: 'primary', type: 'button', onclick: openAddForm }, '+ Add URL'),
    ])];
  }
  return state.sources.map((source) => {
    const observedVia = source.adapter_kind !== source.kind && source.adapter_kind !== 'web'
      ? ` · observed via ${KIND_LABEL[source.adapter_kind] || source.adapter_kind}` : '';
    const job = { source, existing: true, report: null, error: null };
    return h('div', { class: 'source' },
      h('div', null,
        h('div', { class: 'source-title' }, source.title || host(source.canonical_url)),
        h('div', { class: 'source-url' }, externalLink(source.original_url))),
      h('span', { class: `stage ${source.stage}` }, source.stage.replace('_', ' ')),
      h('div', { class: 'source-meta' },
        `${KIND_LABEL[source.kind] || source.kind}${observedVia} · added ${ago(source.discovered_at)} · last observed ${ago(source.last_observed_at)}`,
        source.stage_detail && source.stage !== 'failed' ? ` · ${source.stage_detail}` : ''),
      source.stage === 'failed'
        ? h('div', { class: 'source-error' }, h('span', null, source.stage_detail || source.last_error_message || 'Observation failed'),
            h('button', { class: 'ghost', type: 'button', onclick: () => { state.jobs.set(source.id, job); observe(job); } }, 'Retry'))
        : h('div', null, h('button', { class: 'subtle', type: 'button', onclick: () => { state.jobs.set(source.id, job); observe(job); } }, 'Observe now')));
  });
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
      h('h3', null, node.context.name, h('span', { class: 'kind' }, node.context.kind)),
      node.context.description ? h('p', { class: 'desc' }, node.context.description) : null,
      node.related.length ? h('div', { class: 'chips' }, node.related.map((r) => h('span', { class: 'chip via' }, `↔ ${r.label}`))) : null,
      node.signals.length
        ? h('ul', { class: 'tree' },
            direct.map((s) => h('li', null, h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.subject), ` — ${s.change}`)),
            indirect.map((s) => h('li', { class: 'indirect' }, h('button', { class: 'link', type: 'button', onclick: () => openSignal(s.id) }, s.subject), ` — ${s.change} (through a related context)`)))
        : h('p', { class: 'muted small' }, 'Nothing observed about this yet.')));
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
    h('section', null,
      h('h3', null, 'Why does Stream think this matters?'),
      h('p', null, signal.why_it_matters || 'It isn’t connected to anything you’ve told Stream you care about yet. Add context and Stream will re-evaluate it.'),
      summary.connected_to.length ? h('div', { class: 'chips' }, summary.connected_to.map(chip)) : null),
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
  $('#add-form').addEventListener('submit', (event) => {
    event.preventDefault();
    const input = $('#add-url');
    const url = input.value.trim();
    if (!url) return;
    input.value = '';
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
}

wireForms();
render();
refresh();
