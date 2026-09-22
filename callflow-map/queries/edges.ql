/**
 * @name Complete call flow edges (precise + name-matched)
 * @description Distinct caller->callee edges among first-party functions.
 *   kind = "resolved"     : CodeQL's precise call graph (getACallee).
 *   kind = "name-matched" : heuristic — an otherwise-unresolved call whose
 *                           method/callee name equals a first-party function
 *                           name (captures dynamic dispatch via prototype
 *                           mixins / cross-module method calls).
 * @kind table
 * @id custom/edges
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

int cline(StmtContainer c) { result = c.getLocation().getStartLine() }

predicate preciseEdge(StmtContainer caller, Function callee) {
  exists(DataFlow::InvokeNode n |
    n.getContainer() = caller and
    callee = n.getACallee() and
    firstParty(n.getFile()) and
    firstParty(callee.getFile())
  )
}

predicate nameEdge(StmtContainer caller, Function callee) {
  exists(DataFlow::InvokeNode n, string m |
    n.getContainer() = caller and
    firstParty(n.getFile()) and
    not exists(n.getACallee()) and
    (
      m = n.asExpr().(MethodCallExpr).getMethodName()
      or
      m = n.asExpr().(CallExpr).getCalleeName()
    ) and
    callee.getName() = m and
    firstParty(callee.getFile())
  ) and
  not preciseEdge(caller, callee)
}

from StmtContainer caller, Function callee, string kind
where
  preciseEdge(caller, callee) and kind = "resolved"
  or
  nameEdge(caller, callee) and kind = "name-matched"
select caller.getFile().getRelativePath() as caller_file, cname(caller) as caller_fn,
  cline(caller) as caller_line, callee.getFile().getRelativePath() as callee_file,
  cname(callee) as callee_fn, cline(callee) as callee_line, kind
