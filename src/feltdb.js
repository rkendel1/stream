import { configureDevelopmentRuntimeBridge, createFeltDB } from '@feltdb/core';

export const STREAM_COLLECTIONS = {
  Source: 'Source',
  Item: 'Item',
  ItemSource: 'ItemSource',
  ItemRelation: 'ItemRelation',
  ItemState: 'ItemState',
  Rule: 'Rule',
  RuleExecution: 'RuleExecution',
  Subscription: 'Subscription',
  SemanticDecision: 'SemanticDecision',
  AttentionEvent: 'AttentionEvent',
  FetchAttempt: 'FetchAttempt',
  Provenance: 'Provenance'
};

export function configureStreamDevelopmentBridge(env = detectEnvironment()) {
  if (!env.VITE_FELTDB_DEV_SESSION_ID && !env.VITE_FELTDB_NAMESPACE && !env.VITE_FELTDB_RUNTIME) {
    return false;
  }

  configureDevelopmentRuntimeBridge({
    sessionId: env.VITE_FELTDB_DEV_SESSION_ID,
    workspaceId: env.VITE_FELTDB_WORKSPACE_ID,
    namespace: env.VITE_FELTDB_NAMESPACE,
    runtime: env.VITE_FELTDB_RUNTIME,
    authorityUrl: env.VITE_FELTDB_AUTHORITY_URL,
    bridgeUrl: env.VITE_FELTDB_DEV_BRIDGE_URL,
    applicationUrl: env.VITE_FELTDB_APPLICATION_URL
  });

  return true;
}

export function createStreamDatabase(options = {}) {
  const database = options.db ?? createFeltDB();
  return {
    db: database,
    collections: Object.fromEntries(Object.entries(STREAM_COLLECTIONS).map(([key, name]) => [key, database.collection(name)]))
  };
}

function detectEnvironment() {
  const processEnv = globalThis.process?.env ?? {};
  const viteEnv = typeof import.meta !== 'undefined' && import.meta?.env ? import.meta.env : {};
  return { ...processEnv, ...viteEnv };
}
