/**
 * @name Local requires and their resolution
 * @kind table
 * @id custom/requires
 */
import javascript

predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}

from Require r, string target, string resolved
where
  firstParty(r.getFile()) and
  target = r.getImportedPath().getValue() and
  target.matches(".%") and  // local relative import
  (
    if exists(r.getImportedModule())
    then resolved = r.getImportedModule().getFile().getRelativePath()
    else resolved = "*** UNRESOLVED ***"
  )
select r.getFile().getRelativePath() as from_file,
  r.getLocation().getStartLine() as line, target, resolved
