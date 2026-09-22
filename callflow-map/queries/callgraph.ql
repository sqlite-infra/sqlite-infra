/**
 * @name Call flow map (resolved call graph)
 * @description Every resolved caller -> callee edge in first-party source.
 * @kind table
 * @id custom/callgraph
 */

import javascript

/** A human-readable name for a function-like container. */
string containerName(StmtContainer c) {
  c instanceof TopLevel and result = "<module-toplevel>"
  or
  result = c.(Function).getName()
  or
  c instanceof Function and not exists(c.(Function).getName()) and result = "<anonymous>"
}

/** True if the file is first-party source (not node_modules / externs). */
predicate firstParty(File f) {
  exists(f.getRelativePath()) and
  not f.getRelativePath().matches("node_modules/%")
}

from
  DataFlow::InvokeNode invoke, StmtContainer caller, Function callee, File callerFile,
  File calleeFile
where
  caller = invoke.getContainer() and
  callee = invoke.getACallee() and
  callerFile = invoke.getFile() and
  calleeFile = callee.getFile() and
  firstParty(callerFile) and
  firstParty(calleeFile)
select callerFile.getRelativePath() as caller_file, containerName(caller) as caller_fn,
  invoke.getAstNode().getLocation().getStartLine() as call_line,
  calleeFile.getRelativePath() as callee_file, containerName(callee) as callee_fn,
  callee.getLocation().getStartLine() as callee_line
