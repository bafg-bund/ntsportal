

# run just the gap-filling on all ufids.

library(dplyr)
index = "g2_nts1_bfg"
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

ufidsToGapFill <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid
DBI::dbDisconnect(udb)

for (uf in ufidsToGapFill) {
  ntsportal::es_ufid_gap_fill(escon, index, ufid_to_fill = uf,
                         min_number = config::get("min_number_gap_fill"),
                         mztol_gap_fill_mda = config::get("mztol_gap_fill_mda"),
                         rttol_gap_fill_min = config::get("rttol_gap_fill_min"))

  message("Gap-fill complete for ufid ", uf)
}

message("completed all ufids")
