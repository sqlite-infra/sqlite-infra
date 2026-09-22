/**
 * @name Functions that actually execute at load
 * @kind table
 * @id custom/execfns
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
string nm(Function f){ result = f.getName() or (not exists(f.getName()) and result = "<anonymous>") }
from Function f
where execAtLoad(f) and firstParty(f.getFile())
select f.getFile().getRelativePath() as file, f.getLocation().getStartLine() as line, nm(f) as name
