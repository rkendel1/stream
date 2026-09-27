import test from 'node:test';
import assert from 'node:assert/strict';

import { createSourceRecord, createItemRecord, searchItems, summarizeAttention } from '../src/stream.js';
import { createMemoryStreamService, createStreamApplication } from '../src/appport.js';
import { formatDoctorReport, runCli } from '../src/cli.js';

test('createItemRecord fingerprints equivalent content deterministically', () => {
  const left = createItemRecord({
    source: 'source_1',
    source_kind: 'rss',
    title: 'Rust release',
    content_text: 'A new release is available.',
    canonical_url: 'https://example.com/rust'
  }, '2026-09-27T00:00:00.000Z');

  const right = createItemRecord({
    source: 'source_2',
    source_kind: 'rss',
    title: 'Rust release',
    content_text: 'A new release is available.',
    canonical_url: 'https://example.com/rust'
  }, '2026-09-27T00:00:00.000Z');

  assert.equal(left.fingerprint, right.fingerprint);
  assert.equal(left.canonical_identity, 'https://example.com/rust');
});

test('searchItems filters by query and source kind', () => {
  const items = [
    createItemRecord({ source: 'a', source_kind: 'github', title: 'AppPort released', content_text: 'Capability runtime update' }),
    createItemRecord({ source: 'b', source_kind: 'rss', title: 'Rust async news', content_text: 'Executor updates' })
  ];

  const results = searchItems(items, 'rust async', { source_kind: 'rss' });
  assert.equal(results.length, 1);
  assert.equal(results[0].title, 'Rust async news');
});

test('AppPort manifest exposes Stream capability surface', () => {
  const application = createStreamApplication();
  const capabilityNames = application.manifest().capabilities.map(capability => capability.name).sort();
  assert.deepEqual(capabilityNames, ['appport.manifest', 'appport.ping', 'stream.attention', 'stream.item', 'stream.query', 'stream.search', 'stream.source']);
});

test('memory service deduplicates ingested items by fingerprint', async () => {
  const service = createMemoryStreamService({
    sources: [createSourceRecord({ kind: 'rss', endpoint: 'https://example.com/feed.xml' })]
  });

  await service.item({ action: 'ingest', item: { source: 'source_a', source_kind: 'rss', title: 'Hello', content_text: 'World' } });
  const second = await service.item({ action: 'ingest', item: { source: 'source_b', source_kind: 'rss', title: 'Hello', content_text: 'World' } });

  assert.equal(second.items.length, 1);
  assert.equal(summarizeAttention(second.items).total, 1);
});

test('CLI surfaces doctor and source add flows', async () => {
  const service = createMemoryStreamService();
  const added = await runCli(['source', 'add', 'https://example.com/feed.xml', 'rss'], service);
  const listed = await runCli(['source', 'list'], service);

  assert.match(added, /Added rss source/);
  assert.match(listed, /https:\/\/example.com\/feed.xml/);
  assert.match(formatDoctorReport(), /source failures/);
});
