

# Script to add spectral database to NTSPortal for data viewing

# Creates json of all entries, deletes current index of entries and replaces it with the current
# json. The json is saved under ~/sqlite_local/json

# nohup Rscript update-spectral-library-ntsp.R &> ~/logs/sdb-update-$(date +%y%m%d).log &

library(dplyr)
library(lubridate)
library(DBI)
library(parallel)
library(logger)

VERSION <- "2022-12-21"
SDBPATH <- "~/sqlite_local/MS2_db_v9.db"
INDEX <- "g2_spectral_library"
CONFIG <- "~/config.yml"
INGESTPTH <- "~/projects/ntsautoeval/ingest.sh"

jsonName <- glue::glue("~/sqlite_local/json/{basename(SDBPATH)}-{format(Sys.Date(), '%y%m%d')}.json")

log_info("----- update-spectral-library-ntsp.R v{VERSION} -----")
log_info("Converting sqlite spectral database at {SDBPATH} to json and updating {INDEX} index")

sdb <- DBI::dbConnect(RSQLite::SQLite(), SDBPATH)
# dbListTables(sdb)
# dbListFields(sdb, "retention_time")
# dbListFields(sdb, "fragment")
# dbListFields(sdb, "expGroupExp")
# tbl(sdb, "retention_time") %>% collect()

exps <- tbl(sdb, "experiment") %>%
  left_join(tbl(sdb, "compound"), by = "compound_id") %>%
  left_join(tbl(sdb, "parameter"), by = "parameter_id") %>%
  collect()

exps <- split(exps, seq_len(nrow(exps)))
exps <- lapply(exps, unclass)
# correct names
exps <- lapply(exps, function(doc) {
  names(doc) <- sub("CAS", "cas", names(doc))
  names(doc) <- sub("SMILES", "smiles", names(doc))
  names(doc) <- sub("polarity", "pol", names(doc))
  names(doc) <- sub("^CE$", "ce", names(doc))
  names(doc) <- sub("^CES$", "ces", names(doc))
  doc
})

log_info("Converting MS2")
# first load ms2 table into memory for speed
ms2tbl <- tbl(sdb, "fragment") %>% collect()
exps <- mclapply(exps, function(doc) { #doc <- fts[[1]]
  ms2 <- ms2tbl %>% filter(experiment_id == !!doc$experiment_id) %>%
    select(mz, int)
  # make spectrum relative to max int
  ms2$int <- ms2$int / max(ms2$int)
  ms2 <- split(ms2, seq_len(nrow(ms2)))
  ms2 <- lapply(ms2, unlist)
  ms2 <- unname(ms2)
  ms2 <- lapply(ms2, as.list)
  doc$ms2 <- ms2
  doc
}, mc.cores = 16)

log_info("Converting ret. times")
rttbl <- tbl(sdb, "retention_time") %>% collect()
exps <- mclapply(exps, function(doc) { #doc <- exps[[1]]
  rtt <- rttbl %>% filter(compound_id == !!doc$compound_id) %>%
    select(rt, chrom_method, predicted) 
  colnames(rtt) <- sub("chrom_method", "method", colnames(rtt))
  rtt$doi <- rtt$method
  rtt$predicted <- as.logical(rtt$predicted)
  rtt[!grepl("^dx.doi", rtt$doi), "doi"] <- NA
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
}, mc.cores = 16)


log_info("Converting experiment groups (data_source)")
egetbl <- tbl(sdb, "expGroupExp") %>% collect()
egtbl <- tbl(sdb, "experimentGroup") %>% collect()
exps <- mclapply(exps, function(doc) { #doc <- exps[[1]]
  expGroup <- egetbl %>%
    filter(experiment_id == !!doc$experiment_id) %>%
    left_join(egtbl, by = "experimentGroup_id") 
  doc$data_source <- expGroup$name
  doc
}, mc.cores = 16)

log_info("Converting compound groups")
cgctbl <- tbl(sdb, "compGroupComp") %>% collect()
cgtbl <- tbl(sdb, "compoundGroup") %>% collect()
exps <- mclapply(exps, function(doc) { #doc <- exps[[1]]
  compGroup <- cgctbl %>%
    filter(compound_id == !!doc$compound_id) %>%
    left_join(cgtbl, by = "compoundGroup_id") 
  
  compGroup <- subset(compGroup, Negate(is.element)(name, c("BfG", "LfU", "UBA")))

  if (nrow(compGroup) == 0) {
    return(doc)
  } else {
    doc$comp_group <- compGroup$name
    return(doc)
  }
}, mc.cores = 16)

#which(sapply(exps, function(doc) doc$data_source == "LfU"))

#View(exps[[20007]])
# remove fields (clean up)
exps <- unname(exps)

exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  doc$chem_list_id <- NULL
  doc$experiment_id <- NULL
  doc$parameter_id <- NULL
  doc$compound_id <- NULL

  names(doc) <- sub("col_type", "frag_type", names(doc))
  names(doc) <- sub("isotope", "isotopologue", names(doc))

  doc
})

# put time_added into comment field
exps <- lapply(exps, function(doc) { #doc <- exps[[1]]
  doc$comment <- paste("Original time added to sqlite", doc$time_added)
  doc$time_added <- NULL
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
  if ("rtt" %in% names(doc)) {
    doc$rtt <- lapply(doc$rtt, function(entry) {
    entry$rt <- as.numeric(entry$rt)
    entry$predicted <- as.logical(entry$predicted)
    entry
    })
  }
  doc
})

# remove NA values (again, just in case)
exps <- lapply(exps, function(doc) { # doc <- exps[[1]]
  doc$rtt <- lapply(doc$rtt, function(doc2) Filter(function(x) x != "NA", doc2))
  doc
})
exps <- lapply(exps, function(doc) Filter(Negate(is.na), doc))
exps <- lapply(exps, function(doc) Filter(function(x) length(x) != 0, doc))

log_info("Conversion complete, writing JSON")
jsonlite::write_json(exps, jsonName, pretty = T, digits = NA, auto_unbox = T)

DBI::dbDisconnect(sdb)


# delete current contents of index g2_spectral_library index
log_info("delete current contents of {INDEX} index")

ec <- config::get("elastic_connect", file = CONFIG)

escon <- elastic::connect(
  host = 'elastic.dmz.bafg.de', 
  port = 443, user=ec$user, 
  pwd  = ec$pwd,
  transport_schema = "https"
)

if (escon$ping()$cluster_name != "bfg-elastic-cluster") {
  stop("Connection to es-db not established")
}

res <- elastic::docs_delete_by_query(
  escon, INDEX, 
  body = 
    '
      {
        "query": {
          "match_all": {}
        }
      }
    '
  )

if (length(res$failures) == 0) {
  log_info("Sucessful deletion of {res$deleted} entries")
} else {
  stop("Error in deletion of current index")
} 

log_info("Ingest new spectra using {INGESTPTH}")

system(glue::glue("{INGESTPTH} {CONFIG} {INDEX} {jsonName}"))

log_info("----- End of script update-spectral-library-ntsp.R -------")
