import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  allowAllAuthorizer,
  createApplication,
  createClient,
  createInProcessTransport,
  createServer,
  defineCapability,
  s,
} from '@appport/sdk';

const root = fileURLToPath(new URL('..', import.meta.url));
execFileSync('cargo', ['build', '-q', '-p', 'stream-cli'], { cwd: root, stdio: 'inherit' });
const stream = join(root, 'target', 'debug', process.platform === 'win32' ? 'stream.exe' : 'stream');
const dataDir = mkdtempSync(join(tmpdir(), 'stream-appport-'));
const env = { ...process.env, STREAM_ROOT: root, STREAM_FELTDB_PATH: dataDir };
test.after(() => rmSync(dataDir, { recursive: true, force: true }));

function rust(...args) {
  return execFileSync(stream, args, { cwd: root, env, encoding: 'utf8' });
}

const rustManifest = JSON.parse(rust('appport', 'manifest'));

// Each AppPort capability is backed by the Rust AppPort surface — the same
// `invoke` path the desktop app and the CLI use. No second implementation.
// (The SDK provides the reserved appport.* capabilities itself.)
const capabilities = rustManifest.capabilities.filter((capability) => capability.name.startsWith('stream.')).map((capability) =>
  defineCapability({
    name: capability.name,
    version: capability.version,
    input: s.record(s.unknown()),
    output: s.unknown(),
    authorizationContract: { required: false, public: true },
    effect: capability.effect,
    handler: async (input) => {
      const envelope = JSON.parse(rust('appport', 'invoke', capability.name, JSON.stringify(input ?? {})));
      if (!envelope.ok) throw new Error(`${envelope.error.code}: ${envelope.error.message}`);
      return envelope.result;
    },
  }),
);

const app = createApplication({
  application: { id: rustManifest.application.id, name: 'Stream', version: rustManifest.application.version },
  capabilities,
  // Local, single-user harness: every authenticated caller is the user.
  authorizer: allowAllAuthorizer(),
});

test('Rust manifest exposes the Stream AppPort capability surface', () => {
  const rustCapabilities = rustManifest.capabilities.map((capability) => capability.name).sort();
  const sdkCapabilities = app.manifest().capabilities
    .map((capability) => capability.name)
    .filter((name) => name === 'appport.manifest' || name === 'appport.ping' || name.startsWith('stream.'))
    .sort();
  assert.deepEqual(rustCapabilities, sdkCapabilities);
  for (const name of [
    'stream.source.add', 'stream.source.list', 'stream.item.get', 'stream.item.list',
    'stream.signal.get', 'stream.signal.list', 'stream.context.list', 'stream.context.add',
    'stream.connection.list', 'stream.chat.ask', 'stream.reason.retrieve', 'stream.insight.save',
    'stream.target.parse', 'stream.target.add', 'stream.target.list', 'stream.target.get', 'stream.target.discover',
    'stream.target.pause', 'stream.target.resume', 'stream.target.sources', 'stream.observation.status', 'stream.observation.run',
  ]) {
    assert.ok(rustCapabilities.includes(name), `${name} is part of the surface`);
  }
});

test('an AppPort client drives Stream through the Rust runtime and FeltDB', async () => {
  const server = createServer({ application: app });
  const client = createClient({ transport: createInProcessTransport({ server }) });
  await client.connect();
  try {
    const context = await client.call('stream.context.add', { name: 'Portable compute', kind: 'interest' });
    assert.equal(context.name, 'Portable compute');

    // Durable before anything is fetched; no network needed for this check.
    const added = await client.call('stream.source.add', { url: 'https://www.example.com/article/?utm_source=x', observe: false });
    assert.equal(added.source.stage, 'queued');
    assert.equal(added.source.canonical_url, 'https://example.com/article');
    const again = await client.call('stream.source.add', { url: 'example.com/article', observe: false });
    assert.equal(again.existing, true);
    assert.equal(again.source.id, added.source.id);

    // Observation targets: the client sends the user's /* syntax and gets the
    // parsed url and scope back separately.
    const target = await client.call('stream.target.add', { url: 'https://x.com/devxritesh/status/*', observe: false });
    assert.equal(target.target.scope, 'descendants');
    assert.equal(target.target.url, 'https://x.com/devxritesh/status/');
    assert.equal(target.target.identity.provider, 'x');
    assert.equal(target.target.identity.display_name, '@devxritesh');
    assert.match(rust('targets'), /https:\/\/x\.com\/devxritesh\/status\/\*\s+descendants/);

    // The same durable state is visible to the CLI, a separate process.
    assert.match(rust('context', 'list'), /Portable compute/);
    assert.match(rust('sources'), /https:\/\/example\.com\/article/);

    // Reasoning through the portable surface: a structured, grounded answer.
    const answer = await client.call('stream.chat.ask', { question: 'What connects to portable compute?' });
    assert.equal(answer.sufficiency, 'insufficient', 'nothing observed yet, and Stream says so');
    assert.deepEqual(answer.statements, []);
    assert.ok(answer.summary.includes("don't have enough evidence"));
    assert.equal(typeof answer.retrieved.evidence_count, 'number');
    const status = await client.call('stream.intelligence.status', {});
    assert.equal(status.model_backed, false, 'no model is required to boot');

    const contexts = await client.call('stream.context.list', {});
    assert.equal(contexts.length, 1);
    assert.deepEqual(await client.call('stream.signal.list', {}), []);
  } finally {
    await client.close();
    await server.close();
  }
});
