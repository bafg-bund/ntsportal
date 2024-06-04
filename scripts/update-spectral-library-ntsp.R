
# Script to add spectral database to NTSPortal for data viewing

# Creates json of all entries, deletes current index of entries and replaces it with the current
# json. The json is saved under ~/sqlite_local/json


# nohup Rscript scripts/update-spectral-library-ntsp.R &> /scratch/nts/logs/$(date +%y%m%d)-speclib-update.log &


library(dplyr)
library(lubridate)
library(DBI)
library(parallel)
library(logger)

source("~/connect-ntsp.R")

# Variables to change ##################
INDEX_NAME <- "ntsp_index_spectral_library_v240503"
SPECLIBPATH <- "/scratch/nts/MS2_db_v9.db"
ALIAS <- "ntsp_spectral_library"
CONFIG <- "~/config.yml"
INGESTPTH <- "scripts/ingest.sh"
VERSION <- "2024-05-03"  # of script
# - ###########################################
log_info("----- update-spectral-library-ntsp.R v{VERSION} -----")
log_info("Converting sqlite spectral database at {SPECLIBPATH} to json and updating {ALIAS} index")

jsonName <- glue::glue("/scratch/nts/json/{basename(SPECLIBPATH)}-{format(Sys.Date(), '%y%m%d')}.json")


sdb <- DBI::dbConnect(RSQLite::SQLite(), SPECLIBPATH)

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

# Create new index 

res <- ntsportal::put_spectral_library_index(escon, INDEX_NAME)

if (res$acknowledged) {
  log_info("Ingest new spectra using {INGESTPTH}")
  system(glue::glue("{INGESTPTH} {CONFIG} {INDEX_NAME} {jsonName}"))
  
  # Change alias to new index
  ntsportal::es_move_alias(escon, INDEX_NAME, ALIAS)
  
} else {
  stop("error in index creation")
}

log_info("----- End of script update-spectral-library-ntsp.R -------")


# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.
