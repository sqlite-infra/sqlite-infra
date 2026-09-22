'use strict';

const crypto = require('crypto');

// A database is identified to the subsystems that name objects after it (queue,
// sync, and the keepalive signal) by a stable hash of its name rather than by
// its path: file paths leak tenant, customer and environment names surprisingly
// often. ':memory:' has nothing to hide.
function databaseId(name, anonymous) {
  if (anonymous) return 'memory';
  try {
    return crypto.createHash('sha256').update(String(name)).digest('hex').slice(0, 16);
  } catch {
    return 'unknown';
  }
}

module.exports = { databaseId };
