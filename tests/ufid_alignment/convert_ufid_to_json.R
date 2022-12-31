

# add ufid db to elastic for data viewing

# create json of all entries
library(dplyr)

message("Converting ufid db to json format on ", date())
udb <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/ufid1.sqlite")

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

jsonlite::write_json(fts, "ufid_db.json", pretty = T, digits = NA, auto_unbox = T)

DBI::dbDisconnect(udb)

message("Completed conversion of ufid-db to json format on ", date())
