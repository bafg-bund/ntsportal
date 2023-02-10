

# Run just the gap-filling on all ufids.

# nohup Rscript gap-fill-all-ufids.R g2_nts_bfg &> ~/log-files/gap-fill-all-ufids-$(date +%y%m%d).log &

library(dplyr)
library(logger)
source("~/connect-ntsp.R")

index <- commandArgs(TRUE)
#index <- "g2_nts_bfg"
#index <- "g2_nts_lanuv"

VERSION <- "2023-01-14"
PATHUFID <- "~/sqlite_local/ufid1.sqlite"

log_info("----- gap-fill-all-ufids.R v{VERSION} ----")
log_info("Index: {index}")

udb <- DBI::dbConnect(RSQLite::SQLite(), PATHUFID)
ufidsToGapFill <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid
DBI::dbDisconnect(udb)
totUpdated <- 0

for (uf in ufidsToGapFill) {
  updated <- ntsportal::es_ufid_gap_fill(
    escon, index, ufid_to_fill = uf,
    min_number = config::get("min_number_gap_fill"),
    mztol_gap_fill_mda = config::get("mztol_gap_fill_mda"),
    rttol_gap_fill_min = config::get("rttol_gap_fill_min")
  )
  totUpdated <- totUpdated + updated

  log_info("Gap-fill complete for ufid {uf}")
}
log_info("Total docs updated: {totUpdated}")
log_info("----- Completed gap-fill-all-ufids.R ----")
