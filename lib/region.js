'use strict';

// better-sqlite-adapters re-exports regionFromEnv from here rather than carrying
// a second copy.

const { env: ENV } = require('./dictionary');

/**
 * Region for the current environment, or null.
 *
 * Read only from the standard AWS_REGION / AWS_DEFAULT_REGION environment
 * variables, in that precedence order. No files are read and no home directory
 * is touched: the environment is the only source.
 */
function regionFromEnv(env = process.env) {
  const region = String(env[ENV.region] || env[ENV.regionDefault] || '').trim();
  return region || null;
}

module.exports = { regionFromEnv };
