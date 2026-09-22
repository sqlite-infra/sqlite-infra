# Require detail — `lib/util.js` and `lib/utils.js`

_Generated 2026-09-17 with CodeQL 2.27.0 against the current source (154 first-party files)._

**Rules applied:** counts are unique per file (a file requiring the target N times = 1); a file requiring itself is not counted; `caller` = a file that `require`s the subject; `callee` = a module the subject `require`s (first-party resolves to a `.js`; Node built-ins shown as external).

| file | callers (require) | callees (require) |
|---|---:|---:|
| `lib/util.js` | 24 | 1 |
| `lib/utils.js` | 2 | 4 |

## `lib/util.js`

### Callers — 24 files that `require('./util')`

| # | caller file | require line |
|---:|---|---:|
| 1 | `lib/audit.js` | 21 |
| 2 | `lib/backup-manager.js` | 26 |
| 3 | `lib/batch.js` | 18 |
| 4 | `lib/bulk.js` | 18 |
| 5 | `lib/consumer.js` | 41 |
| 6 | `lib/database.js` | 44 |
| 7 | `lib/diff.js` | 20 |
| 8 | `lib/export.js` | 17 |
| 9 | `lib/importer.js` | 40 |
| 10 | `lib/interfaces.js` | 38 |
| 11 | `lib/kv.js` | 16 |
| 12 | `lib/lock.js` | 17 |
| 13 | `lib/migrate.js` | 24 |
| 14 | `lib/model.js` | 17 |
| 15 | `lib/pipeline.js` | 27 |
| 16 | `lib/queue.js` | 49 |
| 17 | `lib/retry.js` | 12 |
| 18 | `lib/scheduler.js` | 28 |
| 19 | `lib/scope.js` | 17 |
| 20 | `lib/statement.js` | 7 |
| 21 | `lib/sync.js` | 60 |
| 22 | `lib/utils.js` | 5 |
| 23 | `lib/views.js` | 18 |
| 24 | `lib/wrap.js` | 5 |

### Callees — 1 modules `lib/util.js` requires

| # | require path | resolves to | line |
|---:|---|---|---:|
| 1 | `./params` | `lib/params.js` | 19 |

## `lib/utils.js`

### Callers — 2 files that `require('./utils')`

| # | caller file | require line |
|---:|---|---:|
| 1 | `lib/database.js` | 38 |
| 2 | `lib/statement.js` | 3 |

### Callees — 4 modules `lib/utils.js` requires

| # | require path | resolves to | line |
|---:|---|---|---:|
| 1 | `./codes` | `lib/codes.js` | 4 |
| 2 | `./keepalive` | `lib/keepalive.js` | 6 |
| 3 | `./sqlite-error` | `lib/sqlite-error.js` | 3 |
| 4 | `./util` | `lib/util.js` | 5 |

---
Raw data: `util-utils-require-detail.csv` (columns: subject, role, other, resolved, line).
