/**
 * @name Load-time execution graph (function granularity)
 * @description The functions that actually run on require(), and the invocation
 *   edges among the executing containers (module top-level -> fn, fn -> fn).
 * @kind table
 * @id custom/execgraph
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
  exists(Function f | f = c |
    result = f.getFile().getRelativePath() + "::" + f.getLocation().getStartLine() + ":" +
      (f.getName() + "()") 
  )
}
string cidAnon(StmtContainer c) {
  // fallback name for anonymous functions
  c instanceof Function and not exists(c.(Function).getName()) and
  result = c.getFile().getRelativePath() + "::" + c.getLocation().getStartLine() + ":<anonymous>()"
}
// invocation edges where the caller executes at load AND callee is a first-party function
from StmtContainer caller, Function callee, string cs, string ce
where
  exists(DataFlow::InvokeNode call |
    execAtLoad(caller) and caller = call.getContainer() and callee = call.getACallee() and
    firstParty(call.getFile()) and firstParty(callee.getFile())
  ) and
  (cs = cid(caller) or (not exists(cid(caller)) and cs = cidAnon(caller))) and
  (ce = cid(callee) or (not exists(cid(callee)) and ce = cidAnon(callee)))
select cs as from_container, ce as to_container,
  caller.getFile().getRelativePath() as from_file, callee.getFile().getRelativePath() as to_file
