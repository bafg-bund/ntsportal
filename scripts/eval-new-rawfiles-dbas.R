

# Script to process remaining files in msrawfiles, which have not been processed yet.

# WARNING: Make a copy of this document for changes and testing, do not edit this script 
# (or even better, create a branch)

# The script will create batches from the new files by splitting the data by 
# directory
# If no new blanks are in the batches, these will be added to using the blanks
# that are already in the directory from which the files were added

# If files need to be reprocessed, this can be done by removing the field
# "dbas_last_eval" from the msrawfiles doc. This means that any data from
# this file will be removed in dbas index and the file will be reprocessed 
# (including ingest).

# usage (from ntsportal):
# nohup Rscript scripts/eval-new-rawfiles-dbas.R &> /scratch/nts/logs/$(date +%F)_dbas_eval.log &
# tail -f /scratch/nts/logs/$(date +%F)_dbas_eval.log

# If there is an error in processing a file, you can use the function 
# ntsportal::reset_eval, which will remove the last processing date and
# and therefore the file will be processed again

# Variables ####
VERSION <- "2024-04-18"
RFINDEX <- "ntsp_msrawfiles"
TEMPSAVE <- "/scratch/nts/tmp"
CONFG <- "~/config.yml"
INGESTPTH <- "scripts/ingest.sh"
UPDATESPECDB <- "scripts/update-spectral-library-ntsp.R"
ADDANALYSIS <- "scripts/compute-analysis-index.R"
ROOTDIR_RF <- "/scratch/nts/messdaten"
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

log_info("--------- eval-new-rawfiles-dbas.R v{VERSION} -----------")

# Overall checks ####
stopifnot(
  file.exists(CONFG),
  file.exists(INGESTPTH),
  file.exists(UPDATESPECDB),
  file.exists(ADDANALYSIS),
  file.exists(SPECLIBPATH)
)
stopifnot(CORES == 1 || CORESBATCH == 1)
# debug(check_integrity_msrawfiles)
check_integrity_msrawfiles(escon = escon, rfindex = RFINDEX, locationRf = ROOTDIR_RF)

# Collect rawfiles ####
resp <- es_search_paged(escon, RFINDEX, searchBody = '
  {
    "query": {
      "bool": {
        "must_not": [
          {
            "exists": {
              "field": "dbas_last_eval"
            }
          },
          {
            "term": {
              "blank": true
            }
          }
        ]
      }
    },
  "_source": ["path"]
  }
', sort = "path:asc")
hits <- resp$hits$hits
dirs <- unique(dirname(vapply(hits, function(x) x[["_source"]]$path, character(1))))

# Reprocess the entire directory
# Each batch will be made up of the entire directory in which the file is found
# and must contain at least one blank
allFlsSpl <- lapply(dirs, function(pth) { # pth <- dirs[12]
  res2 <- elastic::Search(
    escon, RFINDEX, source = c("blank", "path"), size = 10000, 
    body = list(
      query = list(
        regexp = list(
          path = paste0(pth, "/[^/]*")
        )
      )
    )
  )
  h <- res2$hits$hits
  df <- data.frame(
    id = vapply(h, function(x) x[["_id"]], character(1)),
    index = vapply(h, function(x) x[["_index"]], character(1)),
    path = vapply(h, function(x) x[["_source"]]$path, character(1)),
    blank = vapply(h, function(x) x[["_source"]]$blank, logical(1))
  )
  if (!any(df$blank)) {
    log_warn("No blanks found in {pth}, will not be processed")
    return(NULL)
  } else {
    df$dir <- dirname(df$path)
    df$base <- basename(df$path)
    return(df)
  }
})

allFlsSpl <- Filter(Negate(is.null), allFlsSpl)
stopifnot(all(sapply(allFlsSpl, function(x) length(unique(x$dir))) == 1))

log_info("Processing {length(allFlsSpl)} batches, {nrow(do.call('rbind', allFlsSpl))} files")
allFlsIds <- lapply(allFlsSpl, function(x) x$id)

# Check batches
log_info("Checking batches for consistency")

batchOK <- check_batches_eval(escon = escon, rfindex = RFINDEX, batches = allFlsIds)
stopifnot(batchOK)
log_info("Complete")

log_info("currently {free_gb()} GB of memory available")

# Remove the data in the dbas indices ####
# Only data which are associated with the files to be processed will be removed
# Get filenames of IDs
allFlsNames <- lapply(allFlsSpl, function(x) x$base)
allFlsIndex <- vapply(
  allFlsIds, ntsportal::get_field, indexName = RFINDEX, 
  fieldName = "dbas_index_name", escon = escon, justone = T, character(1)
)

# Test up to here

# Create indices if they do not exist yet
indices <- unique(allFlsIndex)
indPres <- elastic::cat_indices(escon, index = "ntsp_index_dbas*", parse = T)[,3]
indices <- indices[!is.element(indices, indPres)]
for (i in indices) {
  put_dbas_index(escon, i)
}

# Any old results files must be removed (to avoid duplications)
# TODO It would be better to save a list of IDs for deletion and delete them
# after the processing is complete.
log_info("Removing old results in dbas indices")
delRes <- mapply(
  ntsportal::es_remove_by_filename, 
  index = allFlsIndex, 
  filenames = allFlsNames, 
  MoreArgs = list(escon = escon),
  SIMPLIFY = T
)

# Delete previous temp files

system("rm -f /scratch/nts/tmp/*")

# Testing: Reduce batches to those with a small size for testing
# table(sapply(allFlsIds, length))
# which(sapply(allFlsIds, length) == 4)
# allFlsIndex <- allFlsIndex[which(sapply(allFlsIds, length) == 4)] 
# allFlsIds <- allFlsIds[which(sapply(allFlsIds, length) == 4)]
# allFlsNames <- allFlsNames[which(sapply(allFlsNames, length) == 4)]

#######################
# Start processing ####
log_info("Begin processing")

numPeaksBatch <- parallel::mclapply(
  allFlsIds, 
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

# Add data to newly created docs ####
# TODO this should be integrated into the original doc creation
# Use filenames to find out which docs are new

sdb <- con_sqlite(SPECLIBPATH)
res1 <- mapply(es_add_comp_groups, index = allFlsIndex, filenames = allFlsNames, 
       MoreArgs = list(escon = escon, sdb = sdb))
res2 <- mapply(es_add_identifiers, index = allFlsIndex, filenames = allFlsNames, 
               MoreArgs = list(escon = escon, sdb = sdb))
DBI::dbDisconnect(sdb)

# Add analysis index ####
if (any(grepl("_upb", allFlsIndex)))
  system2("Rscript", ADDANALYSIS)

endTime <- lubridate::now()
hrs <- round(as.numeric(endTime - startTime, units = "hours"))

log_info("--------- Completed eval-new-rawfiles-dbas.R in {hrs} h ------------")



