'use strict';

// better-sqlite-adapters re-exports regionFromFile from here rather than carrying
// a second copy.

const fs = require('fs');
const os = require('os');
const path = require('path');
const { env: ENV, fallbacks: FALLBACKS } = require('./dictionary');

// `region = <value>` inside the active section, built once from the key name.
const REGION_LINE = new RegExp(`^${ENV.aws.regionKey}\\s*=\\s*(.+)$`);

/**
 * Region for the active profile, or null.
 *
 * Sections are `[default]` and `[profile name]`: the config file requires the
 * `profile ` prefix on everything except default, and the AWS SDK's own loader
 * ignores sections that omit it, so an unprefixed section is not a match here
 * either. Any failure is a null, never a throw - a telemetry field must not be
 * able to break a database open.
 */
function regionFromFile(env = process.env) {
  const wanted = String(env[ENV.awsProfile] || '').trim() || FALLBACKS.name;
  const heading = wanted === FALLBACKS.name ? FALLBACKS.name : `${ENV.aws.profilePrefix}${wanted}`;
  let text;
  try {
    const dir = env[ENV.awsConfigDir] || path.join(os.homedir(), ENV.aws.dir);
    text = fs.readFileSync(path.join(dir, ENV.aws.config), 'utf8');
  } catch {
    return null;
  }
  let inSection = false;
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.replace(/[;#].*$/, '').trim();
    if (!line) continue;
    const section = /^\[(.+)\]$/.exec(line);
    if (section) { inSection = section[1].trim().replace(/\s+/g, ' ') === heading; continue; }
    if (!inSection) continue;
    const pair = REGION_LINE.exec(line);
    if (pair) return pair[1].trim() || null;
  }
  return null;
}

module.exports = { regionFromFile };
