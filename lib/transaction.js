'use strict';

// db.transaction(fn): wraps `fn` so every call runs inside a transaction.
// Same contract as better-sqlite3: the returned function commits on return
// and rolls back on throw, nested calls become SAVEPOINTs, and `.deferred`,
// `.immediate`, `.exclusive` pick the BEGIN flavour. Async functions are
// rejected because the transaction would end before they finish.

const SAVEPOINT = '`\t_better_sqlite.\t`';
const controllers = new WeakMap();

module.exports = function transaction(fn) {
  if (typeof fn !== 'function') throw new TypeError('Expected first argument to be a function');

  const controller = getController(this);
  const { apply } = Function.prototype;

  const properties = {
    default: { value: wrap(apply, fn, this, controller.default) },
    deferred: { value: wrap(apply, fn, this, controller.deferred) },
    immediate: { value: wrap(apply, fn, this, controller.immediate) },
    exclusive: { value: wrap(apply, fn, this, controller.exclusive) },
    database: { value: this, enumerable: true }
  };

  Object.defineProperties(properties.default.value, properties);
  Object.defineProperties(properties.deferred.value, properties);
  Object.defineProperties(properties.immediate.value, properties);
  Object.defineProperties(properties.exclusive.value, properties);

  return properties.default.value;
};

// One set of control statements per database, prepared on first use. They are
// raw node:sqlite statements, executed through db._run() so the same guards
// apply as for any Statement#run().
function getController(db) {
  let controller = controllers.get(db);
  if (!controller) {
    const prepare = (sql) => db._raw(sql);
    const shared = {
      commit: prepare('COMMIT'),
      rollback: prepare('ROLLBACK'),
      savepoint: prepare(`SAVEPOINT ${SAVEPOINT}`),
      release: prepare(`RELEASE ${SAVEPOINT}`),
      rollbackTo: prepare(`ROLLBACK TO ${SAVEPOINT}`)
    };
    controllers.set(db, controller = {
      default: { begin: prepare('BEGIN'), ...shared },
      deferred: { begin: prepare('BEGIN DEFERRED'), ...shared },
      immediate: { begin: prepare('BEGIN IMMEDIATE'), ...shared },
      exclusive: { begin: prepare('BEGIN EXCLUSIVE'), ...shared }
    });
  }
  return controller;
}

function wrap(apply, fn, db, { begin, commit, rollback, savepoint, release, rollbackTo }) {
  return function sqliteTransaction() {
    let before;
    let after;
    let undo;
    if (db.inTransaction) {
      before = savepoint;
      after = release;
      undo = rollbackTo;
    } else {
      before = begin;
      after = commit;
      undo = rollback;
    }
    db._run(before);
    try {
      const result = apply.call(fn, this, arguments);
      if (result && typeof result.then === 'function') {
        throw new TypeError('Transaction function cannot return a promise');
      }
      db._run(after);
      return result;
    } catch (err) {
      if (db.inTransaction) {
        db._run(undo);
        if (undo !== rollback) db._run(after);
      }
      throw err;
    }
  };
}
