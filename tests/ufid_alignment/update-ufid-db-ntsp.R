

# Add ufid db to ntsp for data viewing
# usage
# nohup Rscript update-ufid-db-ntsp.R &> ~/log-files/update-ufid-db-ntsp-$(date +%y%m%d).log &


library(dplyr)
library(logger)

VERSION <- "2023-01-13"
UFIDDB <- "~/sqlite_local/ufid1.sqlite"
UFIDJSON <- "~/sqlite_local/json/ufid1.json"
INDEX <- "g2_ufid1"
INGESTPTH <- "~/projects/ntsautoeval/ingest.sh"
CONFIG <- "~/config.yml"

log_info("----- update-ufid-db-ntsp.R v{VERSION} -----")
source("~/connect-ntsp.R")

log_info("There are {elastic::count(escon, INDEX)} docs in index {INDEX}")

log_info("Converting ufid-db {UFIDDB} to json format")

udb <- DBI::dbConnect(RSQLite::SQLite(), UFIDDB)

fts <- tbl(udb, "feature") %>% left_join(tbl(udb, "retention_time"), by = "ufid") %>% collect()
fts <- split(fts, seq_len(nrow(fts)))
fts <- lapply(fts, unclass)
ms1t <- tbl(udb, "ms1") %>% collect()
ms2t <- tbl(udb, "ms2") %>% collect()

# add ms1
fts <- lapply(fts, function(doc) { #doc <- fts[[1]]
  ms1 <- ms1t %>% filter(ufid == !!doc$ufid) %>%
    select(mz, rel_int) %>% rename(int = rel_int)
  ms1 <- split(ms1, seq_len(nrow(ms1)))
  ms1 <- lapply(ms1, unlist)
  ms1 <- unname(ms1)
  ms1 <- lapply(ms1, as.list)
  doc$ms1 <- ms1
  doc
})

# add ms2
fts <- lapply(fts, function(doc) { #doc <- fts[[1]]
  ms2 <- ms2t %>% filter(ufid == !!doc$ufid) %>%
    select(mz, rel_int) %>% rename(int = rel_int)
  ms2 <- split(ms2, seq_len(nrow(ms2)))
  ms2 <- lapply(ms2, unlist)
  ms2 <- unname(ms2)
  ms2 <- lapply(ms2, as.list)
  doc$ms2 <- ms2
  doc
})
# clean up
fts <- lapply(fts, function(doc) {
  names(doc) <- sub("date_added", "date_import", names(doc))
  names(doc) <- sub("compound_name", "name", names(doc))
  names(doc) <- sub("polarity", "pol", names(doc))
  doc$date_import <- round(doc$date_import)
  doc
})
fts <- unname(fts)

fts <- lapply(fts, function(doc) Filter(Negate(is.na), doc))
fts <- lapply(fts, function(doc) Filter(function(x) length(x) != 0, doc))

jsonlite::write_json(fts, UFIDJSON, pretty = T, digits = NA, auto_unbox = T)

DBI::dbDisconnect(udb)

log_info("Completed conversion of ufid-db to {UFIDJSON}")

log_info("Clear current {INDEX} index")

res <- elastic::docs_delete_by_query(escon, INDEX, body = '
{
  "query": {
    "match_all": {}
  }
}
')

# res <- elastic::docs_delete_by_query(escon, INDEX, body = '
# {
#   "query": {
#     "term": {
#       "ufid": {
#         "value": 5000
#       }
#     }
#   }
# }
# ')

log_info("{res$deleted} docs deleted")

log_info("Ingest of {UFIDJSON} using {INGESTPTH}")

system(glue::glue("{INGESTPTH} {CONFIG} {INDEX} {UFIDJSON}"))

log_info("There are {elastic::count(escon, INDEX)} docs in index {INDEX}")
log_info("---- Completed update-ufid-db-ntsp.R -----")
