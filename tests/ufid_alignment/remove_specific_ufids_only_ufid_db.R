

# remove specific ufids from ufid-db only

library(DBI)
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
dbListTables(udb)
ufidsToRemove <- c(
  6139
)

for (uf in ufidsToRemove) {
  dbExecute(udb, sprintf("
          DELETE FROM feature
          WHERE ufid = %i;
          ", uf))
}

# test it

library(dplyr)

tbl(udb, "ms2") %>% filter(ufid == !!ufidsToRemove[1]) %>% collect()



DBI::dbDisconnect(udb)
