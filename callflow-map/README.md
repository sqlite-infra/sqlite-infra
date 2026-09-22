# Call flow map — better-sqlite3 (node:sqlite implementation)

Generated with **CodeQL CLI 2.27.0** (JavaScript extractor + `javascript-all` library)
against the whole repository. This directory is the complete, reproducible call
flow map: the CodeQL queries, the raw edge data, and an interactive viewer.

## TL;DR

*Last regenerated 2026-09-18 against the current source (153 first-party files).*

- **The code is healthy for call-flow extraction.** All 153 first-party JS/TS
  files extracted cleanly — **0 parse errors**, **0 broken `require()`s**. No code
  changes were needed to produce a complete map. (The only "unresolved" imports are
  three `require('../package.json')` calls, which are JSON data, not call targets.)
- **New since the last regeneration:** `lib/utils.js` was added (env/schema
  validation, atomic writes, diagnostics for `keepalive`'s `configure()`/`describe()`),
  and **`lib/keepalive.js` is now orphaned** — no module in the codebase requires it
  any more (verified both by `grep` and by CodeQL's require graph: every edge
  touching `keepalive.js` has it as the *source*, requiring `identity.js`,
  `region.js`, `sinks.js`, `utils.js`, `dictionary.js` — never as a *target*). It no
  longer appears anywhere in the load-time or execution-tree scopes. See
  [What runs on `require('better-sqlite3')`](#what-runs-on-requirebetter-sqlite3-load-time-scope).
- The map has two layers, each edge tagged with a `kind`:
  - **`resolved`** — CodeQL's precise call graph (`DataFlow::InvokeNode.getACallee()`).
    High confidence. **4,755 distinct function→function edges**, from 5,886 resolved
    first-party call sites (6,044 call-site→callee rows in `call-sites-resolved.csv`,
    since one call site can resolve to more than one callee).
  - **`name-matched`** — a heuristic overlay that recovers the dynamically-dispatched
    calls CodeQL's precise engine cannot follow (see below). **22,699 edges.**
    Over-approximate; use as a lead, not proof.

## Why two layers were necessary

CodeQL precisely resolves **7,903 / 19,745** first-party call sites (40%). The other
60% are *legitimately* unresolvable by static analysis and are **not** code defects:

| Unresolved bucket | Examples | Count (top) |
|---|---|---|
| Test framework / assertions | `expect` (1,277), `.equal` (1,546), `.throw`, `test`, `.deepEqual`, `.throws`, `.ok` | ~4,700 |
| JS / Node built-ins | `.push`, `.map`, `.join`, `.slice`, `.from`, `new Error`, `.existsSync` | ~2,000 |
| `require()` itself | module loader, not a call edge | 518 |
| **First-party API methods (dynamic dispatch)** | `.prepare` (396), `.get`, `.all`, `.pluck`, `.aggregate`, `.run`, `.table`, `.iterate`, `.pragma`, `.exec`, `.transaction`, `.close`, `.where` | ~2,500 |

The last bucket is the interesting one. Those method names **do** correspond to real
first-party functions, but they are attached to `Database.prototype` /
`Statement.prototype` through runtime mixins:

```js
// lib/database.js
Database.prototype.transaction = require('./transaction');
Object.assign(Database.prototype,
  require('./introspect'), require('./maintenance'), require('./bulk'),
  require('./batch'), require('./schema-builder'), require('./model'), ...);
```

A call like `db.prepare(sql)` in a test therefore has a receiver whose type CodeQL
cannot track back across the `Object.assign` boundary, so `getACallee()` returns
nothing. This is a fundamental limit of static call-graph resolution for JS dynamic
dispatch — **not a bug in the code.** The `name-matched` layer closes that gap by
linking each such call to every first-party function of the same name (hence
over-approximate, e.g. a `.get` call links to *every* first-party function named `get`).

## Headline architecture (resolved layer, module level)

Library dependency hubs — highest fan-in among `lib/`+`adapters/`+`index.js`
(callers, excluding self-loops and test files):

| Module | fan-in | fan-out | role |
|---|---:|---:|---|
| `lib/database.js` | 30 | 5 | the `Database` class; central hub |
| `lib/schema.js` | 27 | 2 | schema definition, used across builders |
| `lib/statement.js` | 26 | 3 | the `Statement` class |
| `lib/util.js` | 21 | 0 | leaf helpers (guards, option parsing) |

Most-exercised library modules from the test/conformance suites (resolved
test→lib edges): `lib/database.js` (991), `lib/statement.js` (337),
`lib/queue.js` (185), `lib/sync.js` (130), `lib/consumer.js` (68).

Resolved module-level graph used by the viewer: **140 modules, 333 edges.**

## What runs on `require('better-sqlite3')` (load-time scope)

`require('better-sqlite3')` loads `index.js` (the package `main`) and runs its
top-level, which transitively `require()`s the rest of the library. But **that runs
module-*initialization* code, not the API logic.** CodeQL's load-time reachability
(`loadflow.ql` — seed at `index.js`'s module top-level, follow `require()` into each
imported module's top-level, and follow calls made from load-executing code):

| At `require()` time | Count |
|---|---:|
| Module top-levels executed (the require closure) | **64 of 155** |
| Functions that actually run | **5 of 4,819** (~0.1%) |
| `require()` load edges | 145 |

The 5 functions that execute are `keepalive.createAgent`/`safe`, `dictionary.deepFreeze`
(recursive), and `schema.quote` — plus module top-level statements (feature detection
like `'isTransaction' in DatabaseSync.prototype`, `Object.assign(Database.prototype, …)`
prototype wiring, and frozen constant tables). **Every API method (`prepare`, `run`,
`transaction`, `model`, …) is only *defined* at load; it runs later, on use.** One extra
function, `collision.report()`, runs *deferred* via `setImmediate` on the next tick.

This slice is isolated as its own graph (`data/graph-loadtime.json`, 57 nodes / 145
require edges — `index.js` + 56 `lib/` modules, no tests or adapters) and as the
**"Load-time modules"** scope toggle in `callflow.html`. In the runtime view, modules
that initialize at load are marked with a dashed ring.

### The 5 functions that actually execute (finest scope)

The require closure above runs *module top-levels*. Only **5 of 4,819 functions** run
a body during that. They are (`execfns.ql` / `execgraph.ql`):

| Function | file:line | Why it runs at load |
|---|---|---|
| *(anonymous IIFE)* | `lib/database.js:577` | `(({union,unionAll,intersect,except})=>({…}))(require('./setops'))` — invoked inline to pick set-op builders while wiring the query DSL |
| `createAgent` | `lib/keepalive.js:118` | top-level `globalThis[KEY] \|\| (globalThis[KEY] = createAgent())` — builds the process-wide keepalive agent once |
| `safe` | `lib/keepalive.js:147` | called *by* `createAgent` |
| `quote` | `lib/schema.js:14` | called from a module top-level to build a constant quoted-identifier string |
| `deepFreeze` | `lib/dictionary.js:40` | top-level `const dictionary = deepFreeze({…})` — recursively freezes the default dictionary (self-recursive) |

Every other function — every `Database`/`Statement` API method — is only *defined* at
load and runs on use. One additional function, `collision.report()`, runs **deferred**
on the next tick via `setImmediate` (it is `require`d and called only inside that
callback, so it is outside the synchronous load slice; CodeQL does not model
`setImmediate` invoking its argument, so this path is noted by hand).

In `callflow.html` this is the default scope, the **"Execution tree"** — a collapsible
tree **rooted at `index.js`, in true execution order** (each `require()` runs a
module's top-level depth-first; the 5 real function executions ● branch off where they
are called). Ordering comes from source line numbers (`loadorder.ql`), so it matches
what Node actually does: `index.js → database.js → backend → sqlite-error → statement →
…`. Paths to the 5 executing functions are pre-expanded; everything else is collapsed
(click to expand). A module required a second time appears as a faded reference
(already initialized — Node caches it).

Underlying data: `data/exec-tree.json` (151-node hierarchy: 57 modules initialized +
89 already-initialized references + 5 function activations), plus the flat
`data/graph-execfns.json` / `data/exec-functions.csv` (5 functions + their direct
call/require edges).

## Files

### `data/`
| File | What |
|---|---|
| `edges-all.csv` | Every distinct function→function edge, `kind` = resolved \| name-matched (27,320 rows) |
| `edges-resolved.csv` | Precise edges only (4,704 rows) |
| `call-sites-resolved.csv` | Per-call-site precise edges with call-site line numbers (5,983 rows) |
| `module-callgraph.csv` | Function edges aggregated to `from_module,to_module,kind,edge_count` (all files) |
| `lib-callgraph.csv` | Same, restricted to `lib/`+`adapters/`+`index.js` |
| `lib-module-degree.csv` | Per-module fan-in / fan-out |
| `require-graph.csv` | First-party `require()` dependency graph (374 edges) |
| `loadflow.csv` | Load-time edges — what executes on `require('better-sqlite3')` (call + require, per `loadflow.ql`) |
| `exec-functions.csv` | The invocation edges among the 5 functions that actually execute at load (per `execgraph.ql`) |
| `graph-resolved.json` | Node/link graph (resolved, module level); each node flagged `load` = runs at import — powers the runtime scope of `callflow.html` |
| `graph-loadtime.json` | Load-time module scope (57 nodes / 145 require edges) — powers the "Load-time modules" toggle |
| `graph-execfns.json` | Flat executed-functions graph (12 nodes / 12 edges) |
| `exec-tree.json` | **Execution tree** rooted at `index.js` in source order (151-node hierarchy) — powers the default "Execution tree" scope |

### `queries/` — the CodeQL queries used
| Query | Purpose |
|---|---|
| `edges.ql` | **The map.** Precise + name-matched function edges with `kind`. |
| `loadflow.ql` | **Load-time slice.** Code reachable when `require('better-sqlite3')` runs (require closure + calls from load-executing code). |
| `execfns.ql` | The functions that actually execute at load (file:line + name). |
| `execgraph.ql` | The invocation edges among load-executing containers (top-level → fn, fn → fn). |
| `loadorder.ql` | Load-time edges **with source line numbers**, so the execution tree is built in the order Node runs it. |
| `callgraph.ql` | Per-call-site precise edges. |
| `modrequire.ql` | First-party require graph. |
| `coverage.ql` | Resolution coverage counters. |
| `unresolved.ql` | Buckets unresolved call sites by callee name (diagnosis). |
| `defs.ql` | First-party function definitions by inferred name. |
| `requires.ql` | Local `require()` targets and whether they resolve (broken-import check). |
| `diag.ql` | `JSParseError` count per file (extraction health). |

### `callflow.html`
Self-contained interactive viewer (embedded data). Three scopes:
**Execution tree** (default — collapsible tree from `index.js` of what runs on
`require()`), **Runtime call graph** (force-directed resolved calls), and
**Load-time modules** (the require closure). Open directly in a browser.

## Reproduce

```bash
# 1. CodeQL CLI 2.27.0 bundle (includes the JS extractor + libraries)
#    https://github.com/github/codeql-action/releases/tag/codeql-bundle-v2.27.0

# 2. Build the database (from the repo root), excluding node_modules
export LGTM_INDEX_FILTERS=$'exclude:node_modules\nexclude:**/node_modules'
codeql database create /tmp/bsql-db --language=javascript --source-root=. --overwrite

# 3. Run any query and decode to CSV
codeql query run callflow-map/queries/edges.ql --database=/tmp/bsql-db --output=/tmp/edges.bqrs
codeql bqrs decode /tmp/edges.bqrs --format=csv --output=edges-all.csv
```

## Caveats

- **`name-matched` is over-approximate.** An edge there means "a call whose name
  matches this function exists," not "this exact function is invoked." Filter to
  `kind == resolved` for a sound (but incomplete) graph.
- Calls resolving into `*.d.ts` declaration files are normalized to their `.js`
  implementation in the module-level views.
- Reflection/`eval`-style dispatch (none material here) is invisible to both layers.
