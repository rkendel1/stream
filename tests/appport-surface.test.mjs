import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createApplication, defineCapability, s } from '@appport/sdk';

const streamSource = defineCapability({
  name: 'stream.source',
  version: 1,
  input: s.object({ operation: s.string() }),
  output: s.record(s.unknown()),
  authorizationContract: { required: false, public: true },
  effect: 'consequential',
  handler: async () => ({ ok: true })
});
const streamItem = defineCapability({
  name: 'stream.item',
  version: 1,
  input: s.object({ operation: s.string() }),
  output: s.record(s.unknown()),
  authorizationContract: { required: false, public: true },
  effect: 'consequential',
  handler: async () => ({ ok: true })
});
const streamQuery = defineCapability({
  name: 'stream.query',
  version: 1,
  input: s.object({ operation: s.string() }),
  output: s.record(s.unknown()),
  authorizationContract: { required: false, public: true },
  effect: 'observation',
  handler: async () => ({ ok: true })
});
const streamSearch = defineCapability({
  name: 'stream.search',
  version: 1,
  input: s.object({ operation: s.string() }),
  output: s.record(s.unknown()),
  authorizationContract: { required: false, public: true },
  effect: 'observation',
  handler: async () => ({ ok: true })
});
const streamAttention = defineCapability({
  name: 'stream.attention',
  version: 1,
  input: s.object({ operation: s.string() }),
  output: s.record(s.unknown()),
  authorizationContract: { required: false, public: true },
  effect: 'observation',
  handler: async () => ({ ok: true })
});

test('Rust manifest exposes the Stream AppPort capability surface', () => {
  const manifestJson = execFileSync('cargo', ['run', '-q', '-p', 'stream-cli', '--', 'appport', 'manifest'], {
    cwd: '/home/runner/work/stream/stream',
    encoding: 'utf8'
  });
  const rustManifest = JSON.parse(manifestJson);

  const app = createApplication({
    application: {
      id: 'com.rkendel.stream',
      name: 'Stream',
      version: '0.1.0'
    },
    capabilities: [streamSource, streamItem, streamQuery, streamSearch, streamAttention]
  });

  const rustCapabilities = rustManifest.capabilities.map(capability => capability.name).sort();
  const expected = app.manifest().capabilities
    .map(capability => capability.name)
    .filter(name => name === 'appport.manifest' || name === 'appport.ping' || name.startsWith('stream.'))
    .sort();

  assert.deepEqual(rustCapabilities, expected);
});
