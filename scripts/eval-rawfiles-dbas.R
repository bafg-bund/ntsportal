


# Script to process all files in msrawfiles-db
# Equivalent to dbas1.R to ingest. All stages of processing are
# carried out. Once everything is complete the script will 
# change aliases to the newly created indices.

VERSION <- "2023-09-27"

# Variables ####
RFINDEX <- "g2_msrawfiles"
TEMPSAVE <- "/scratch/nts/tmp"
CONFG <- "~/config.yml"
INGESTPTH <- "/scratch/nts/ntsautoeval/ingest.sh"
CORES <- 1
CORESBATCH <- 6

library(ntsworkflow)
library(logger)
library(ntsportal)

startTime <- lubridate::now()
setwd("~/projects/ntsautoeval/msrawfiles-db/")

# Create escon variable
source("~Jewell/connect-ntsp.R")

log_info("--------- eval-rawfiles-dbas.R v{VERSION} -----------")


# Checks ####
stopifnot(file.exists(CONFG))
stopifnot(CORES == 1 || CORESBATCH == 1)

# check presence of library
resp2 <- elastic::Search(escon, RFINDEX, body = '
                {
  "query": {
    "match_all": {}
  },
  "aggs": {
    "csl_loc": {
      "terms": {
        "field": "dbas_spectral_library",
        "size": 10
      }
    }
  },
  "size": 0
}
                ')

buc <- resp2$aggregations$csl_loc$buckets
cslp <- sapply(buc, function(x) x$key)
stopifnot(all(file.exists(cslp)), all(grepl("\\.db$", cslp)))
for (pth in cslp) {
  dbtest <- DBI::dbConnect(RSQLite::SQLite(), pth)
  if (!DBI::dbExistsTable(dbtest, "experiment"))
    stop("DB not of the correct format")
  DBI::dbDisconnect(dbtest)
}

# check presence of IS tables
resp3 <- elastic::Search(escon, RFINDEX, body = '
                {
  "query": {
    "match_all": {}
  },
  "aggs": {
    "ist_loc": {
      "terms": {
        "field": "dbas_is_table",
        "size": 100
      }
    }
  },
  "size": 0
}
                ')

buc <- resp3$aggregations$ist_loc$buckets
istp <- sapply(buc, function(x) x$key)
stopifnot(all(file.exists(istp)), all(grepl("\\.csv$", istp)))

# check db integrity


# check that these fields exist in all docs
stopifnot(
  check_field(escon, RFINDEX, "dbas_is_table"),
  check_field(escon, RFINDEX, "dbas_area_threshold"),
  check_field(escon, RFINDEX, "dbas_rttolm"),
  check_field(escon, RFINDEX, "dbas_mztolu"),
  check_field(escon, RFINDEX, "dbas_mztolu_fine"),
  check_field(escon, RFINDEX, "dbas_ndp_threshold"),
  check_field(escon, RFINDEX, "dbas_rtTolReinteg"),
  check_field(escon, RFINDEX, "dbas_ndp_m"),
  check_field(escon, RFINDEX, "dbas_ndp_n"),
  check_field(escon, RFINDEX, "dbas_instr"),
  check_field(escon, RFINDEX, "pol"),
  check_field(escon, RFINDEX, "chrom_method"),
  check_field(escon, RFINDEX, "matrix"),
  check_field(escon, RFINDEX, "data_source"),
  check_field(escon, RFINDEX, "dbas_index_name"),
  check_field(escon, RFINDEX, "blank"),
  check_field(escon, RFINDEX, "path"),
  check_field(escon, RFINDEX, "duration", onlyNonBlank = TRUE)
)

checkLoc <- elastic::Search(escon, RFINDEX, body = '
                {
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "loc"
          }
        }
      ],
      "must": [
        {
          "term": {
            "blank": {
              "value": false
            }
          }
        }
      ]
    }
  }
}
                ')
stopifnot(checkLoc$hits$total$value == 0)


# Collect rawfiles ####
resp <- elastic::Search(escon, RFINDEX, body = 
'
  {
    "query": {
      "match_all": {}
    },
  "size": 10000,
  "_source": ["path"]
  }
'
)
# if there are more than 10000, you need to page over the results
stopifnot(resp$hits$total$relation == "eq")

hits <- resp$hits$hits
allFls <- data.frame(
  id = sapply(hits, function(x) x[["_id"]]),
  path = sapply(hits, function(x) x[["_source"]]$path)
)

fileCheck <- sapply(allFls$path, file.exists)
if (!all(fileCheck)) {
  log_warn("The following files do not exist")
  noFile <- allFls$path[!fileCheck]
  for (i in noFile)
    message(i)
  stop("Missing raw files")
}

allFls$dir <- dirname(allFls$path)
allFls$base <- basename(allFls$path)

# there can not be any duplicated filenames, because the filename is used
# by the alignment to distinguish between files.
# but in blanks it's okay, these will not factor into alignment anyway

if (any(duplicated(allFls$base))) {
  idsDup <- allFls[duplicated(allFls$base), "id"]
  isBlank <- elastic::docs_mget(escon, RFINDEX, ids = idsDup, source = "blank")$docs
  if (all(sapply(isBlank, function(d) d[["_source"]][["blank"]]))) {
    warning("There are duplicated filenames in the db (but only for blanks)")
  } else {
    stop("There are duplicated filenames in the db")  
  }
}

# Create batches for processing
# split list by matrix, station, pol, and then by month in case of daily sampling
# in the case of daily sampling, the batches are separated by the directory
# they are in. The easiest way to split these is by the directory (which is
# coincidentaly also the month)

allFlsSpl <- split(allFls, allFls$dir)
#View(allFlsSpl[[150]])
#allFls <- allFls[sapply(allFls, nrow) != 0]

stopifnot(sum(sapply(allFlsSpl, nrow)) == length(hits))

#allFlsSpl[[200]]
log_info("Processing {length(hits)} files in {length(allFlsSpl)} batches")

allFlsIds <- lapply(allFlsSpl, function(x) x$id)

# Check batches
log_info("Checking batches for consistency")
# for each of these batches, certain fields must be the same
# for all docs, these include:
mustBeSame <- c(
  "dbas_blank_regex",
  "dbas_minimum_detections",
  "dbas_is_table",
  "dbas_is_name",
  "duration",
  "pol",
  "matrix",
  "data_source",
  "chrom_method",
  "dbas_index_name",
  "dbas_alias_name"
)

allSame <- vapply(allFlsIds, function(x) {
  all(vapply(mustBeSame, check_uniformity, esids = x, escon = escon, 
             rfindex = RFINDEX, logical(1)))
}, logical(1))

if (!all(allSame))
  stop("Batch similarity checks have failed")

mustBeSameNonBlanks <- c(
  "dbas_build_averages",
  "dbas_replicate_regex"
)

allSame2 <- vapply(allFlsIds, function(x) {
  all(vapply(mustBeSameNonBlanks, check_uniformity, esids = x, escon = escon, 
             rfindex = RFINDEX, onlyNonBlanks = T, logical(1)))
}, logical(1))

if (!all(allSame2))
  stop("Batch similarity checks have failed")


log_info("Batch checks complete")

# Create new indices ####
resAlia <- elastic::Search(escon, RFINDEX, body = '
 {
    "query": {
      "match_all": {}
    },
    "aggs": {
      "aliases": {
        "terms": {
          "field": "dbas_alias_name"
        }
      }
    },
    "size": 0
  }
  ')$aggregations$aliases$buckets

allAlia <- sapply(resAlia, "[[", i = "key")
allInd <- sapply(allAlia, create_dbas_index, escon = escon, rfIndex = RFINDEX)

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

# Move aliases to new indices ####
log_info("Moving aliases to new indices")
suca <- mapply(move_dbas_alias, indexName = allInd, aliasName = allAlia,
               MoreArgs = list(escon = escon))
if (all(suca))
  log_info("Move alias successful")

endTime <- lubridate::now()
hrs <- round(as.numeric(endTime - startTime, units = "hours"))

log_info("--------- Completed eval-rawfiles-dbas.R in {hrs} h ------------")
