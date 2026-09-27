import { createHash, randomUUID } from 'node:crypto';

export const SOURCE_KINDS = ['rss', 'atom', 'json_feed', 'web', 'github', 'youtube', 'email', 'webhook', 'api', 'appport'];
export const SOURCE_STATUSES = ['discovered', 'active', 'paused', 'failed', 'disabled', 'deleted'];
export const ITEM_STATES = ['unseen', 'seen', 'read', 'saved', 'dismissed', 'important', 'acted_on', 'archived'];
export const FAILURE_CATEGORIES = ['network', 'authentication', 'authorization', 'parsing', 'normalization', 'deduplication', 'storage', 'semantic', 'provider', 'delivery'];

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, sortKeys(value[key])]));
  }
  return value;
}

export function stableStringify(value) {
  return JSON.stringify(sortKeys(value));
}

export function createFingerprint(input) {
  return createHash('sha256').update(stableStringify(input)).digest('hex');
}

function ensureValue(value, field) {
  if (!value || !String(value).trim()) throw new Error(`${field} is required`);
  return String(value).trim();
}

export function createSourceRecord(input, now = new Date().toISOString()) {
  const kind = ensureValue(input.kind, 'kind');
  if (!SOURCE_KINDS.includes(kind)) throw new Error(`Unsupported source kind: ${kind}`);
  const endpoint = ensureValue(input.endpoint, 'endpoint');
  const status = input.status ?? 'active';
  if (!SOURCE_STATUSES.includes(status)) throw new Error(`Unsupported source status: ${status}`);
  const last_error_category = input.last_error_category ?? '';
  if (last_error_category && !FAILURE_CATEGORIES.includes(last_error_category)) {
    throw new Error(`Unsupported failure category: ${last_error_category}`);
  }
  return {
    id: input.id ?? `source_${randomUUID()}`,
    kind,
    endpoint,
    identity: input.identity ?? endpoint,
    capabilities: stableStringify(input.capabilities ?? []),
    refresh_policy: input.refresh_policy ?? 'manual',
    authentication_reference: input.authentication_reference ?? '',
    status,
    last_success_at: input.last_success_at ?? '',
    last_failure_at: input.last_failure_at ?? '',
    last_error_category,
    provenance: input.provenance ?? '',
    created_at: input.created_at ?? now,
    updated_at: input.updated_at ?? now
  };
}

export function createItemRecord(input, now = new Date().toISOString()) {
  const title = ensureValue(input.title, 'title');
  const content_text = (input.content_text ?? '').trim();
  const canonical_url = (input.canonical_url ?? '').trim();
  const source_kind = ensureValue(input.source_kind, 'source_kind');
  if (!SOURCE_KINDS.includes(source_kind)) throw new Error(`Unsupported source kind: ${source_kind}`);
  const fingerprint = input.fingerprint ?? createFingerprint({
    canonical_url,
    title,
    content_text,
    author_name: input.author_name ?? '',
    published_at: input.published_at ?? ''
  });

  return {
    id: input.id ?? `item_${randomUUID()}`,
    source: ensureValue(input.source, 'source'),
    canonical_url,
    title,
    content_text,
    content_html: input.content_html ?? '',
    author_name: input.author_name ?? '',
    published_at: input.published_at ?? '',
    source_kind,
    canonical_identity: input.canonical_identity ?? (canonical_url || fingerprint),
    fingerprint,
    created_at: input.created_at ?? now,
    updated_at: input.updated_at ?? now
  };
}

export function createItemStateRecord(input, now = new Date().toISOString()) {
  const state = ensureValue(input.state, 'state');
  if (!ITEM_STATES.includes(state)) throw new Error(`Unsupported item state: ${state}`);
  return {
    id: input.id ?? `state_${randomUUID()}`,
    item: ensureValue(input.item, 'item'),
    state,
    seen_at: input.seen_at ?? '',
    read_at: input.read_at ?? '',
    saved_at: input.saved_at ?? '',
    dismissed_at: input.dismissed_at ?? '',
    important_at: input.important_at ?? '',
    acted_on_at: input.acted_on_at ?? '',
    archived_at: input.archived_at ?? '',
    updated_at: input.updated_at ?? now
  };
}

export function createFetchAttemptRecord(input, now = new Date().toISOString()) {
  const failure_category = input.failure_category ?? '';
  if (failure_category && !FAILURE_CATEGORIES.includes(failure_category)) {
    throw new Error(`Unsupported failure category: ${failure_category}`);
  }
  return {
    id: input.id ?? `fetch_${randomUUID()}`,
    source: ensureValue(input.source, 'source'),
    status: input.status ?? 'started',
    failure_category,
    started_at: input.started_at ?? now,
    finished_at: input.finished_at ?? '',
    item_count: Number(input.item_count ?? 0),
    duplicate_count: Number(input.duplicate_count ?? 0),
    retained_count: Number(input.retained_count ?? 0),
    error_message: input.error_message ?? ''
  };
}

export function createRuleRecord(input, now = new Date().toISOString()) {
  return {
    id: input.id ?? `rule_${randomUUID()}`,
    name: ensureValue(input.name, 'name'),
    when_expression: ensureValue(input.when_expression, 'when_expression'),
    then_action: ensureValue(input.then_action, 'then_action'),
    retain_policy: input.retain_policy ?? 'default',
    status: input.status ?? 'active',
    explanation: input.explanation ?? '',
    created_at: input.created_at ?? now,
    updated_at: input.updated_at ?? now
  };
}

export function searchItems(items, query = '', filters = {}) {
  const tokens = String(query).trim().toLowerCase().split(/\s+/).filter(Boolean);
  return items.filter(item => {
    if (filters.source && item.source !== filters.source) return false;
    if (filters.source_kind && item.source_kind !== filters.source_kind) return false;
    if (filters.after && item.published_at && item.published_at <= filters.after) return false;
    if (filters.state && item.state !== filters.state) return false;
    const haystack = [item.title, item.content_text, item.author_name, item.canonical_url]
      .filter(Boolean)
      .join(' ')
      .toLowerCase();
    return tokens.every(token => haystack.includes(token));
  });
}

export function summarizeAttention(items) {
  const total = items.length;
  const bySourceKind = Object.fromEntries(SOURCE_KINDS.map(kind => [kind, 0]));
  for (const item of items) {
    if (item?.source_kind && item.source_kind in bySourceKind) bySourceKind[item.source_kind] += 1;
  }
  return { total, bySourceKind };
}
