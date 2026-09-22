/**
 * @name First-party function definitions and inferred names
 * @kind table
 * @id custom/defs
 */
import javascript

predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}

from string name, int cnt
where
  name = any(Function f | firstParty(f.getFile())).getName() and
  cnt = count(Function f | firstParty(f.getFile()) and f.getName() = name) and
  name != ""
select name, cnt order by cnt desc
