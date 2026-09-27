import { createApplication, defineCapability, s } from '@appport/sdk';
import {
  createItemRecord,
  createItemStateRecord,
  createSourceRecord,
  searchItems,
  summarizeAttention
} from './stream.js';

const PUBLIC = { required: false, public: true };

const sourceSchema = s.object({
  id: s.optional(s.string()),
  kind: s.string(),
  endpoint: s.string(),
  identity: s.optional(s.string()),
  capabilities: s.optional(s.array(s.string())),
  refresh_policy: s.optional(s.string()),
  authentication_reference: s.optional(s.string()),
  status: s.optional(s.string()),
  provenance: s.optional(s.string())
});

const itemSchema = s.object({
  id: s.optional(s.string()),
  source: s.string(),
  source_kind: s.string(),
  canonical_url: s.optional(s.string()),
  title: s.string(),
  content_text: s.optional(s.string()),
  content_html: s.optional(s.string()),
  author_name: s.optional(s.string()),
  published_at: s.optional(s.string())
});

const sourceCapability = defineCapability({
  name: 'stream.source',
  version: 1,
  description: 'Manage durable Stream source records.',
  input: s.object({
    action: s.enum(['add', 'list']),
    source: s.optional(sourceSchema)
  }),
  output: s.object({
    sources: s.array(s.record(s.unknown())),
    updated: s.optional(s.record(s.unknown()))
  }),
  authorizationContract: PUBLIC,
  effect: 'consequential',
  handler: async (input, context) => context.services.stream.source(input)
});

const itemCapability = defineCapability({
  name: 'stream.item',
  version: 1,
  description: 'Ingest items and maintain durable item state.',
  input: s.object({
    action: s.enum(['ingest', 'list', 'read', 'set_state']),
    item: s.optional(itemSchema),
    itemId: s.optional(s.string()),
    state: s.optional(s.string())
  }),
  output: s.object({
    items: s.array(s.record(s.unknown())),
    item: s.optional(s.record(s.unknown())),
    state: s.optional(s.record(s.unknown()))
  }),
  authorizationContract: PUBLIC,
  effect: 'consequential',
  handler: async (input, context) => context.services.stream.item(input)
});

const queryCapability = defineCapability({
  name: 'stream.query',
  version: 1,
  description: 'Query normalized Stream items without transport-specific assumptions.',
  input: s.object({
    source: s.optional(s.string()),
    after: s.optional(s.string()),
    state: s.optional(s.string())
  }),
  output: s.object({
    items: s.array(s.record(s.unknown()))
  }),
  authorizationContract: PUBLIC,
  effect: 'observation',
  handler: async (input, context) => context.services.stream.query(input)
});

const searchCapability = defineCapability({
  name: 'stream.search',
  version: 1,
  description: 'Search the durable Stream corpus.',
  input: s.object({
    query: s.string(),
    source: s.optional(s.string()),
    source_kind: s.optional(s.string()),
    after: s.optional(s.string())
  }),
  output: s.object({
    items: s.array(s.record(s.unknown()))
  }),
  authorizationContract: PUBLIC,
  effect: 'observation',
  handler: async (input, context) => context.services.stream.search(input)
});

const attentionCapability = defineCapability({
  name: 'stream.attention',
  version: 1,
  description: 'Summarize the items that require human attention.',
  input: s.object({
    action: s.enum(['summary'])
  }),
  output: s.object({
    attention: s.record(s.unknown())
  }),
  authorizationContract: PUBLIC,
  effect: 'observation',
  handler: async (_input, context) => context.services.stream.attention()
});

export function createMemoryStreamService(seed = {}) {
  const sources = [...(seed.sources ?? [])];
  const items = [...(seed.items ?? [])];
  const itemStates = [...(seed.itemStates ?? [])];

  return {
    async source(input) {
      if (input.action === 'list') return { sources };
      const source = createSourceRecord(input.source ?? {});
      sources.push(source);
      return { sources, updated: source };
    },
    async item(input) {
      if (input.action === 'list') return { items };
      if (input.action === 'read') {
        const item = items.find(candidate => candidate.id === input.itemId);
        return { items, item };
      }
      if (input.action === 'set_state') {
        const state = createItemStateRecord({ item: input.itemId, state: input.state });
        itemStates.push(state);
        return { items, item: items.find(candidate => candidate.id === input.itemId), state };
      }
      const next = createItemRecord(input.item ?? {});
      const duplicate = items.find(candidate => candidate.fingerprint === next.fingerprint);
      if (duplicate) return { items, item: duplicate };
      items.push(next);
      return { items, item: next };
    },
    async query(input) {
      return { items: searchItems(items.map(item => ({ ...item, state: itemStates.find(state => state.item === item.id)?.state })), '', input) };
    },
    async search(input) {
      return { items: searchItems(items, input.query, input) };
    },
    async attention() {
      return { attention: summarizeAttention(items) };
    }
  };
}

export function createStreamApplication(options = {}) {
  const services = {
    stream: options.service ?? createMemoryStreamService(options.seed)
  };

  return createApplication({
    application: {
      id: 'com.rkendel.stream',
      name: 'Stream',
      version: '0.1.0',
      description: 'A local-first information runtime built on FeltDB and AppPort.'
    },
    services,
    capabilities: [sourceCapability, itemCapability, queryCapability, searchCapability, attentionCapability]
  });
}
