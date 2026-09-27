#!/usr/bin/env node
import { fileURLToPath } from 'node:url';

import { createMemoryStreamService } from './appport.js';

export const STREAM_COMMANDS = [
  'stream source add <endpoint> [kind]',
  'stream source list',
  'stream sync',
  'stream item list',
  'stream item read <id>',
  'stream item save <id>',
  'stream item dismiss <id>',
  'stream search <query>',
  'stream rule list',
  'stream rule test <id>',
  'stream export',
  'stream doctor'
];

export function formatDoctorReport(summary = {}) {
  const checks = [
    ['source failures', summary.sourceFailures ?? 'none recorded'],
    ['authentication failures', summary.authenticationFailures ?? 'none recorded'],
    ['parser failures', summary.parserFailures ?? 'none recorded'],
    ['stale sources', summary.staleSources ?? 'none recorded'],
    ['storage problems', summary.storageProblems ?? 'none recorded'],
    ['rule failures', summary.ruleFailures ?? 'none recorded'],
    ['delivery failures', summary.deliveryFailures ?? 'none recorded']
  ];

  return ['Stream doctor', ...checks.map(([label, value]) => `- ${label}: ${value}`)].join('\n');
}

export async function runCli(argv, service = createMemoryStreamService()) {
  const [command, subcommand, ...rest] = argv;

  if (!command || command === '--help' || command === 'help') {
    return ['Stream — local-first information runtime', '', ...STREAM_COMMANDS].join('\n');
  }

  if (command === 'source' && subcommand === 'add') {
    const endpoint = rest[0];
    const kind = rest[1] ?? 'rss';
    const result = await service.source({ action: 'add', source: { endpoint, kind } });
    return `Added ${result.updated.kind} source ${result.updated.endpoint}`;
  }

  if (command === 'source' && subcommand === 'list') {
    const result = await service.source({ action: 'list' });
    return result.sources.map(source => `${source.kind}\t${source.endpoint}\t${source.status}`).join('\n');
  }

  if (command === 'search') {
    const query = rest.join(' ');
    const result = await service.search({ query });
    return result.items.map(item => `${item.id}\t${item.title}`).join('\n');
  }

  if (command === 'doctor') {
    return formatDoctorReport();
  }

  return `Unsupported command: ${[command, subcommand, ...rest].filter(Boolean).join(' ')}`;
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  runCli(process.argv.slice(2)).then(output => {
    if (output) console.log(output);
  }).catch(error => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
