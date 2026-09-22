/**
 * @name Unresolved call sites bucketed by callee text
 * @kind table
 * @id custom/unresolved
 */
import javascript

predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}

predicate unresolvedFP(DataFlow::InvokeNode n) {
  firstParty(n.getFile()) and not exists(n.getACallee())
}

string label(DataFlow::InvokeNode n) {
  result = "method:." + n.asExpr().(MethodCallExpr).getMethodName()
  or
  exists(CallExpr c | c = n.asExpr() and not c instanceof MethodCallExpr |
    result = "call:" + c.getCalleeName()
  )
  or
  result = "new:" + n.asExpr().(NewExpr).getCalleeName()
}

from string lab, int cnt
where
  lab = label(any(DataFlow::InvokeNode m | unresolvedFP(m))) and
  cnt = count(DataFlow::InvokeNode n | unresolvedFP(n) and label(n) = lab) and
  cnt > 0
select lab, cnt order by cnt desc
