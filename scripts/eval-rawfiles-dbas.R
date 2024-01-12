


# Script to process all files in msrawfiles-db
# Equivalent to dbas1.R to ingest. All stages of processing are
# carried out. Once everything is complete the script will 
# change aliases to the newly created indices.

# This script will processes all files and create new indices
# For iterative processing of newly added files, use the script eval-new-rawfiles-dbas.R

# usage (from ntsportal):
# nohup Rscript scripts/eval-rawfiles-dbas.R &> /scratch/nts/logs/$(date +%y%m%d)_dbas_eval.log &
# tail -f /scratch/nts/logs/$(date +%y%m%d)_dbas_eval.log
# see crontab for processing

VERSION <- "2024-01-02"

# Variables ####
RFINDEX <- "g2_msrawfiles"
TEMPSAVE <- "/scratch/nts/tmp"
CONFG <- "~/config.yml"
INGESTPTH <- "/scratch/nts/ntsautoeval/ingest.sh"
UPDATESPECDB <- "~/projects/ntsportal/scripts/update-spectral-library-ntsp.R"
ADDANALYSIS <- "~/projects/ntsportal/scripts/compute-analysis-index.R"
SPECLIBPATH <- "/scratch/nts/MS2_db_v9.db"  # temporary: only for adding group and formula after processing
CORES <- 1
CORESBATCH <- 6

library(ntsworkflow)
library(logger)
library(ntsportal)

startTime <- lubridate::now()
#setwd("~/projects/ntsautoeval/msrawfiles-db/")

# Create escon variable
source("~Jewell/connect-ntsp.R")

log_info("--------- eval-rawfiles-dbas.R v{VERSION} -----------")


# Overall checks ####
stopifnot(file.exists(CONFG))
stopifnot(CORES == 1 || CORESBATCH == 1)
check_integrity_msrawfiles(escon = escon, rfindex = RFINDEX)

# Collect rawfiles ####
resp <- es_search_paged(escon, RFINDEX, searchBody = '
  {
    "query": {
      "match_all": {}
    },
  "_source": ["path"]
  }
', sort = "path:asc")


hits <- resp$hits$hits
allFls <- data.frame(
  id = sapply(hits, function(x) x[["_id"]]),
  path = sapply(hits, function(x) x[["_source"]]$path)
)

allFls$dir <- dirname(allFls$path)
allFls$base <- basename(allFls$path)

# Create batches for processing
# split list by matrix, station, pol, and then by month in case of daily sampling
# in the case of daily sampling, the batches are separated by the directory
# they are in. The easiest way to split these is by the directory (which is
# coincidentally also the month)

allFlsSpl <- split(allFls, allFls$dir)
#View(allFlsSpl[[150]])
#allFls <- allFls[sapply(allFls, nrow) != 0]

stopifnot(sum(sapply(allFlsSpl, nrow)) == length(hits))

#allFlsSpl[[200]]
log_info("Processing {length(hits)} files in {length(allFlsSpl)} batches")

allFlsIds <- lapply(allFlsSpl, function(x) x$id)

# Check batches
log_info("Checking batches for consistency")
check_batches_eval(escon = escon, rfindex = RFINDEX, batches = allFlsIds)
log_info("Complete")

# Create new indices ####
resAlia <- elastic::Search(escon, RFINDEX, body = '
 {
    "query": {
      "match_all": {}
    },
    "aggs": {
      "aliases": {
        "terms": {
          "field": "dbas_alias_name",
          "size": 200
        }
      }
    },
    "size": 0
  }
  ')$aggregations$aliases$buckets

allAlia <- vapply(resAlia, "[[", i = "key", character(1))
allInd <- vapply(allAlia, create_dbas_index, escon = escon, rfIndex = RFINDEX, 
                 character(1))

log_info("currently {free_gb()} GB of memory available")
log_info("Processing files (parallel)")
# run tests using eval-rawfiles-dbas-tests.R


# Process files ####
numPeaksBatch <- parallel::mclapply(
  allFlsIds, #
  proc_batch, 
  escon = escon,
  rfindex = RFINDEX,
  tempsavedir = TEMPSAVE, 
  ingestpth = INGESTPTH, 
  configfile = CONFG,
  coresBatch = CORESBATCH,
  mc.cores = CORES,
  mc.preschedule = FALSE
)
numPeaksBatch <- numPeaksBatch[sapply(numPeaksBatch, is.numeric)]
numPeaksBatch <- as.numeric(numPeaksBatch)

log_info("Completed all batches")
log_info("Average peaks found per batch: {mean(numPeaksBatch)}")
log_info("currently {free_gb()} GB of memory available")

# Transfer current spectral lib to ntsp ####
system2("Rscript", UPDATESPECDB)

# Add information to docs ####
# should be moved to proc_batch function in the future
sdb <- con_sqlite(SPECLIBPATH)

# Add compound group, identifiers
#i <- "g2_dbas_v231006_bfg"
for (i in allInd) {
  es_add_comp_groups(escon, sdb, i)
  es_add_identifiers(escon, sdb, i)
}

DBI::dbDisconnect(sdb)


# Move aliases to new indices ####
log_info("Moving aliases to new indices")
suca <- mapply(es_move_alias, indexName = allInd, aliasName = allAlia,
               MoreArgs = list(escon = escon, closeAfter = TRUE))
if (all(suca))
  log_info("Move aliases successful") else log_error("Move aliases unsuccessful")

# Add analysis index ####
system2("Rscript", ADDANALYSIS)

endTime <- lubridate::now()
hrs <- round(as.numeric(endTime - startTime, units = "hours"))

log_info("--------- Completed eval-rawfiles-dbas.R in {hrs} h ------------")
