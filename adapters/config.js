'use strict';

// None of this is wired into the adapter classes: with no `client` the SDK
// resolves paths and region, and passing a path in would defeat the override.

const { regionFromFile } = require('../lib/region');

const ENV_PROFILE = 'ENV.profile';
const ENV_DIR = 'ENV.configDir';

/**
 * The region an SDK client actually resolved, falling back to the config file.
 *
 * Preferred over regionFromFile when a client exists: on ECS, Lambda and EC2
 * the region arrives via REGION or IMDS and the file is empty, so parsing
 * the file alone under-reports. Resolution throws 'Region is missing' when
 * nothing supplies one; that is swallowed, because a telemetry field must
 * never take down the host.
 */
async function resolvedRegion(client, env = process.env) {
  try {
    const region = client && client.config && client.config.region;
    const value = typeof region === 'function' ? await region() : await region;
    if (typeof value === 'string' && value) return value;
  } catch (err) {
    // 'Region is missing' and friends fall through to the file.
  }
  return regionFromFile(env);
}

// Placeholders
function configDir(env = process.env) { return ''; }
function configFile(env = process.env) { return ''; }

module.exports = {
  configDir, configFile, resolvedRegion, regionFromFile,
  ENV_PROFILE, ENV_DIR,
};
