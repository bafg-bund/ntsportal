

# Rebuild the ufid-db by going through each ufid, clearing the ufid and then
# building the averaged spectra again from scratch.

# Make a backup of the database first!

library(dplyr)
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)


DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
ufidsToRebuild <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid

for (uf in ufidsToRebuild) {
  dbExecute(udb, sprintf("
          DELETE FROM feature
          WHERE ufid = %i;
          ", uf))
  message("cleared ufid ", uf)

  ntsportal::udb_update(udb, escon, "g2_nts*", uf)

  message("Rebuild complete for ufid ", uf)
}


DBI::dbDisconnect(udb)
