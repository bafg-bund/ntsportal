# clear all ufids in es and in ufid_db


index <- "g2_nts*"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)


elastic::docs_update_by_query(escon, index, body =
'
{
  "query": {
    "exists": {
      "field": "ufid"
    }
  },
  "script": {
    "source": "ctx._source.remove(\'ufid\')",
    "lang": "painless"
  }
}
'
)

elastic::docs_update_by_query(escon, index, body =
                                '
{
  "query": {
    "exists": {
      "field": "ucid"
    }
  },
  "script": {
    "source": "ctx._source.remove(\'ucid\')",
    "lang": "painless"
  }
}
'
)


DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")

DBI::dbExecute(udb, "DELETE FROM feature;")
library(dplyr)
tbl(udb, "feature") %>% collect()
tbl(udb, "retention_time") %>% collect()
tbl(udb, "ms1") %>% collect()
tbl(udb, "ms2") %>% collect()
DBI::dbDisconnect(udb)
