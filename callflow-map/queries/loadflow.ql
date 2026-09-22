/**
 * @name Load-time call flow (what runs on require('better-sqlite3'))
 * @kind table
 * @id custom/loadflow
 */
import javascript

predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}
string cname(StmtContainer c) {
  c instanceof TopLevel and result = "<module-toplevel>"
  or
  result = c.(Function).getName()
  or
  c instanceof Function and not exists(c.(Function).getName()) and result = "<anonymous>"
}

/** A container whose code executes during require('better-sqlite3'). */
predicate execAtLoad(StmtContainer c) {
  // seed: index.js module top-level
  c instanceof TopLevel and c.(TopLevel).getFile().getRelativePath() = "index.js"
  or
  // requiring a module (from load-executing code) runs that module's top-level
  exists(Require r | execAtLoad(r.getContainer()) and c = r.getImportedModule())
  or
  // a function called from load-executing code runs at load
  exists(DataFlow::InvokeNode call, Function f |
    execAtLoad(call.getContainer()) and f = call.getACallee() and c = f
  )
}

from string etype, string ff, string fn, string tf, string tn
where
  exists(DataFlow::InvokeNode call, StmtContainer caller, Function callee |
    execAtLoad(caller) and caller = call.getContainer() and callee = call.getACallee() and
    firstParty(call.getFile()) and firstParty(callee.getFile()) and
    etype = "call" and ff = call.getFile().getRelativePath() and fn = cname(caller) and
    tf = callee.getFile().getRelativePath() and tn = cname(callee)
  )
  or
  exists(Require r, Module m |
    execAtLoad(r.getContainer()) and m = r.getImportedModule() and
    firstParty(r.getFile()) and firstParty(m.getFile()) and
    etype = "require" and ff = r.getFile().getRelativePath() and fn = "<module-toplevel>" and
    tf = m.getFile().getRelativePath() and tn = "<module-toplevel>"
  )
select etype, ff, fn, tf, tn
