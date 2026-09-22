/**
 * @name Extraction diagnostics
 * @kind table
 * @id custom/diag
 */
import javascript
from File f, int errs
where
  errs = count(JSParseError e | e.getFile() = f) and
  errs > 0 and
  not f.getRelativePath().matches("node_modules/%")
select f.getRelativePath(), errs
