/**
 * @name Module require graph (first-party)
 * @kind table
 * @id custom/modrequire
 */
import javascript
predicate firstParty(File f) {
  exists(f.getRelativePath()) and not f.getRelativePath().matches("node_modules/%")
}
from Require r, File src, File dst
where
  src = r.getFile() and firstParty(src) and
  dst = r.getImportedModule().getFile() and firstParty(dst)
select src.getRelativePath() as from_module, dst.getRelativePath() as to_module
