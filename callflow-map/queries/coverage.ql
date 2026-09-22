/**
 * @name Call resolution coverage
 * @kind table
 * @id custom/coverage
 */
import javascript

predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}

from string k, int v
where
  k = "total_call_sites_fp" and v = count(DataFlow::InvokeNode n | firstParty(n.getFile()))
  or
  k = "resolved_call_sites_fp" and
    v = count(DataFlow::InvokeNode n | firstParty(n.getFile()) and exists(n.getACallee()))
  or
  k = "resolved_to_firstparty" and
    v = count(DataFlow::InvokeNode n | firstParty(n.getFile()) and exists(Function c | c = n.getACallee() and firstParty(c.getFile())))
  or
  k = "unresolved_call_sites_fp" and
    v = count(DataFlow::InvokeNode n | firstParty(n.getFile()) and not exists(n.getACallee()))
select k, v
