/**
 * @name Load-time execution edges with source order (line numbers)
 * @kind table
 * @id custom/loadorder
 */
import javascript
predicate firstParty(File f) { exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%") }
predicate execAtLoad(StmtContainer c) {
  c instanceof TopLevel and c.(TopLevel).getFile().getRelativePath() = "index.js"
  or
  exists(Require r | execAtLoad(r.getContainer()) and c = r.getImportedModule())
  or
  exists(DataFlow::InvokeNode call, Function f | execAtLoad(call.getContainer()) and f = call.getACallee() and c = f)
}
string cid(StmtContainer c) {
  c instanceof TopLevel and result = c.getFile().getRelativePath() + "::<toplevel>"
  or
  exists(Function f | f = c | result = f.getFile().getRelativePath() + "::" + f.getLocation().getStartLine() + ":" + f.getName() + "()")
  or
  exists(Function f | f = c and not exists(f.getName()) | result = f.getFile().getRelativePath() + "::" + f.getLocation().getStartLine() + ":<anonymous>()")
}
from string from_container, string etype, int line, string to_ref
where
  // require: ordered by the require() call-site line inside its container
  exists(Require r, Module m |
    execAtLoad(r.getContainer()) and m = r.getImportedModule() and
    firstParty(r.getFile()) and firstParty(m.getFile()) and
    from_container = cid(r.getContainer()) and etype = "require" and
    line = r.getLocation().getStartLine() and to_ref = m.getFile().getRelativePath()
  )
  or
  // call: ordered by call-site line
  exists(DataFlow::InvokeNode call, Function callee |
    execAtLoad(call.getContainer()) and callee = call.getACallee() and
    firstParty(call.getFile()) and firstParty(callee.getFile()) and
    from_container = cid(call.getContainer()) and etype = "call" and
    line = call.getAstNode().getLocation().getStartLine() and to_ref = cid(callee)
  )
select from_container, etype, line, to_ref
