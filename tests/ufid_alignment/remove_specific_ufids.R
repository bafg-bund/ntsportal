

# remove specific ufids from ufid-db and es

library(DBI)
index <- "g2_nts1_bfg"
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
ufidsToRemove <- c(
  9999,
  9999
)

for (uf in ufidsToRemove) {
  dbExecute(udb, sprintf("
          DELETE FROM feature
          WHERE ufid = %i;
          ", uf))
  elastic::docs_update_by_query(escon, index, body = sprintf('
  {
    "query": {
      "term": {
        "ufid": {
          "value": %i
        }
      }
    },
    "script": {
        "source": "ctx._source.remove(\'ufid\')",
        "lang": "painless"
    }
  }
  ', uf))
}

# test it

library(dplyr)

tbl(udb, "ms2") %>% filter(ufid == 27) %>% collect()


