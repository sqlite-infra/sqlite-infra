'use strict';

// Not wired into the adapter classes: with no `client` the SDK resolves the
// region itself. No config file or home directory is read here.

const { regionFromEnv } = require('../lib/region');

/**
 * The region an SDK client actually resolved, falling back to the environment.
 *
 * Preferred over regionFromEnv when a client exists: on ECS, Lambda and EC2
 * the region arrives via IMDS, which the client resolves and the environment
 * may not carry. Resolution returns null when nothing supplies one.
 */
async function resolvedRegion(client, env = process.env) {
  try {
    const region = client && client.config && client.config.region;
    const value = typeof region === 'function' ? await region() : await region;
    if (typeof value === 'string' && value) return value;
  } catch (err) {
    // 'Region is missing' and friends fall through to the environment.
  }
  return regionFromEnv(env);
}

module.exports = { resolvedRegion, regionFromEnv };
