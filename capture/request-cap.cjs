// Preload with NODE_OPTIONS=--require=<absolute path>. No credentials are logged.
'use strict';
const fs = require('node:fs');
const crypto = require('node:crypto');
const MAX_REQUESTS = 26;
const remaining = Number(process.env.XBIRD_REMAINING);
const limit = Number.isSafeInteger(remaining) && remaining >= 0 ? Math.min(MAX_REQUESTS, remaining) : MAX_REQUESTS;
const JITTER_MIN_MS = 150;
const JITTER_MAX_MS = 450;
let sent = 0;
let rateLimited = false;
let chain = Promise.resolve();
const original = globalThis.fetch;
if (typeof original !== 'function') throw new Error('xbird requires global fetch');
globalThis.fetch = function cappedFetch(...args) {
  const operation = chain.then(async () => {
    if (rateLimited) throw new Error('xbird: rate_limited');
    if (sent >= limit) throw new Error('xbird: request_cap');
    if (sent) await new Promise(resolve => setTimeout(resolve,
      JITTER_MIN_MS + crypto.randomInt(JITTER_MAX_MS - JITTER_MIN_MS + 1)));
    sent++;
    const response = await original.apply(globalThis, args);
    if (response.status === 429) {
      rateLimited = true;
      throw new Error('xbird: rate_limited');
    }
    return response;
  });
  chain = operation.catch(() => {});
  return operation;
};
process.on('exit', () => {
  if (process.env.XBIRD_RECEIPT) fs.writeFileSync(process.env.XBIRD_RECEIPT,
    JSON.stringify({requests_sent: sent, rate_limited: rateLimited, cap: MAX_REQUESTS, limit}) + '\n', {flag: 'wx', mode: 0o600});
});
