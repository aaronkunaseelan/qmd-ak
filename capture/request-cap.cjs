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
let identityFailed = false;
// viewerQuery from X's startup-C2nOe7yA.js web bundle, 27 Oct 2026.
const VIEWER_QUERY_ID = 'nCB1QyWck5tTItQJP1evzA';
const original = globalThis.fetch;
if (typeof original !== 'function') throw new Error('xbird requires global fetch');
globalThis.fetch = function cappedFetch(...args) {
  const operation = chain.then(async () => {
    if (identityFailed) throw new Error('bird identity unavailable');
    if (rateLimited) throw new Error('xbird: rate_limited');
    if (sent >= limit) throw new Error('xbird: request_cap');
    if (sent) await new Promise(resolve => setTimeout(resolve,
      JITTER_MIN_MS + crypto.randomInt(JITTER_MAX_MS - JITTER_MIN_MS + 1)));
    const identity = String(args[0]) === 'https://x.com/i/api/account/settings.json';
    if (identity) {
      const variables = {__relay_internal__pv__xclientsfeaturesappviewerisloggedinrelayprovider: true};
      args[0] = `https://api.x.com/graphql/${VIEWER_QUERY_ID}/viewerQuery?` +
        new URLSearchParams({variables: JSON.stringify(variables)});
    }
    sent++;
    let response;
    try {
      response = await original.apply(globalThis, args);
      if (identity && response.status !== 429) {
        if (!response.ok) throw new Error('bird identity unavailable');
        const data = await response.json();
        const user = data?.data?.viewer?.user_results?.result;
        if (data.errors?.length || typeof user?.rest_id !== 'string' || !/^[0-9]+$/.test(user.rest_id) ||
            typeof user?.core?.screen_name !== 'string' || !user.core.screen_name) {
          throw new Error('bird identity unavailable');
        }
        response = Response.json({screen_name: user.core.screen_name, user_id: user.rest_id});
      }
    } catch (error) {
      if (!identity) throw error;
      identityFailed = true;
      throw new Error('bird identity unavailable');
    }
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
