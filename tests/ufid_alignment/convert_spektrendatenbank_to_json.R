

# add spektrendatenbank to elastic for data viewing

# create json of all entries
library(dplyr)
library(lubridate)
library(DBI)

# only needs to be done for newly added spectra
afterDate <- ymd("20100101", tz = "Europe/Berlin")

sdb <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v7.db")
# dbListTables(sdb)
# dbListFields(sdb, "retention_time")
# dbListFields(sdb, "fragment")
# dbListFields(sdb, "expGroupExp")
# tbl(sdb, "retention_time") %>% collect()

exps <- tbl(sdb, "experiment") %>%
  left_join(tbl(sdb, "compound"), by = "compound_id") %>%
  left_join(tbl(sdb, "parameter"), by = "parameter_id") %>%
  collect()
exps$time_added <- ymd_hms(exps$time_added, tz = "Europe/Berlin")
exps <- subset(exps, time_added > afterDate)

exps <- split(exps, seq_len(nrow(exps)))
exps <- lapply(exps, unclass)
#exps[[2]]
# correct names
exps <- lapply(exps, function(doc) {
  names(doc) <- sub("CAS", "cas", names(doc))
  names(doc) <- sub("SMILES", "smiles", names(doc))
  names(doc) <- sub("polarity", "pol", names(doc))
  names(doc) <- sub("^CE$", "ce", names(doc))
  names(doc) <- sub("^CES$", "ces", names(doc))
  doc
})

# add ms2
exps <- lapply(exps, function(doc) { #doc <- fts[[1]]
  ms2 <- tbl(sdb, "fragment") %>% filter(experiment_id == !!doc$experiment_id) %>%
    select(mz, int) %>% collect()
  # make spectrum relative to max int
  ms2$int <- ms2$int / max(ms2$int)
  ms2 <- split(ms2, seq_len(nrow(ms2)))
  ms2 <- lapply(ms2, unlist)
  ms2 <- unname(ms2)
  ms2 <- lapply(ms2, as.list)
  doc$ms2 <- ms2
  doc
})

# add rtt
exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  rtt <- tbl(sdb, "retention_time") %>% filter(compound_id == !!doc$compound_id) %>%
    select(rt, chrom_method) %>% collect()
  colnames(rtt) <- sub("chrom_method", "method", colnames(rtt))
  rtt$predicted <- FALSE
  rtt$doi <- rtt$method
  rtt <- subset(rtt, !is.na(method))
  if (nrow(rtt) == 0)
    return(doc)
  rtt[rtt$method == "dx.doi.org/10.1016/j.chroma.2015.11.014", "method"] <- "bfg_nts_rp1"
  rtt <- split(rtt, seq_len(nrow(rtt)))
  rtt <- lapply(rtt, unlist)
  rtt <- unname(rtt)
  rtt <- lapply(rtt, as.list)
  doc$rtt <- rtt
  doc
})


# add experiment groups (data_source)

exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  expGroup <-  tbl(sdb, "expGroupExp") %>%
    filter(experiment_id == !!doc$experiment_id) %>%
    left_join(tbl(sdb, "experimentGroup"), by = "experimentGroup_id") %>%
    collect()
  doc$data_source <- expGroup$name
  doc
})

# add compound groups
exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  compGroup <-  tbl(sdb, "compGroupComp") %>%
    filter(compound_id == !!doc$compound_id) %>%
    left_join(tbl(sdb, "compoundGroup"), by = "compoundGroup_id") %>%
    collect()

  compGroup <- subset(compGroup, Negate(is.element)(name, c("BfG", "LfU")))

  if (nrow(compGroup) == 0) {
    return(doc)
  } else {
    doc$comp_group <- compGroup$name
    return(doc)
  }
})


# remove fields (clean up)
exps <- unname(exps)

exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  doc$chem_list_id <- NULL
  doc$experiment_id <- NULL
  doc$parameter_id <- NULL
  doc$compound_id <- NULL

  names(doc) <- sub("time_added", "date_import", names(doc))
  doc$date_import <- round(as.numeric(doc$date_import))

  names(doc) <- sub("col_type", "frag_type", names(doc))
  names(doc) <- sub("isotope", "isotopologue", names(doc))

  doc
})

# remove NA values
exps <- lapply(exps, function(doc) Filter(Negate(is.na), doc))
exps <- lapply(exps, function(doc) Filter(function(x) length(x) != 0, doc))

# round ms2$int values
exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  doc$ms2 <- lapply(doc$ms2, function(entry) {
    entry$int <- round(entry$int, 4)
    entry
  })
  doc
})

# parse rt as float and predicted as boolean
exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  if (rtt %in% names(doc)) {
    doc$rtt <- lapply(doc$rtt, function(entry) {
    entry$rt <- as.numeric(entry$rt)
    entry$predicted <- as.logical(entry$predicted)
    entry
    })
  }
  doc
})

# remove NA values (again, just in case)
exps <- lapply(exps, function(doc) Filter(Negate(is.na), doc))
exps <- lapply(exps, function(doc) Filter(function(x) length(x) != 0, doc))

jsonlite::write_json(exps, "spektrendatenbank.json", pretty = T, digits = NA, auto_unbox = T)

DBI::dbDisconnect(sdb)
