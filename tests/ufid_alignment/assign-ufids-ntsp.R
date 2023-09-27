
# Process an entire index to assign ufids (level 1) to all features

# usage:
# Rscript assign-ufids-ntsp.R config_file.yml
# The config file contains all the necessary settings.
# e.g.
# nohup Rscript assign-ufids-ntsp.R config.yml &> ~/log-files/ufid-alignment-$(date +%y%m%d).log &
# nohup Rscript ../assign-ufids-ntsp.R config-aug-test.yml &> ufid-alignment-$(date +%y%m%d).log &

library(ntsportal)
library(logger)

# for debugging purposes, isolate steps so that you can start from a predefined point.
# 1: Pos initial pass
# 2: Pos clustering and final pass
# 3: Neg initial pass
# 4: Neg clustering and final pass
STEPS <- 1:4
VERSION <- "2023-09-27"

startTime <- lubridate::now()
configFile <- commandArgs(TRUE)
stopifnot(file.exists(configFile))
#configFile <- "tests/ufid_alignment/config.yml"
#configFile <- "~/projects/ntsportal/tests/ufid_alignment/aug-test/config-aug-test.yml"

# Set environment variable of config file so that in all functions
# these settings are used.
Sys.setenv(R_CONFIG_FILE = configFile)
# check for settings
stopifnot(is.numeric(config::get("ms2_ndp_min_score")))

index <- config::get("index_name")
path_ufid_lib <- config::get("path_ufid_lib")
#index <- "g2_nts_bfg"
#index <- "g2_nts_lanuv"

# Check args
if (!is.character(index) || length(index) != 1 || !grepl("^g2_nts", index))
  stop("Incorrect index specification")

log_info("----- assign-ufids-ntsp.R v{VERSION} -----")
log_info("Processing {index} with {path_ufid_lib}")

# test that config.yml is present (this is not the same config.yml used for ntsp password!)
# it must be located in the current working dir and have all the settings needed for alignment.
if (is.null(config::get("mztol_rough_mda")))
  stop("no config.yml with settings found")

# print settings
message("**Settings**")
setin <- config::get()
message(paste(paste(names(setin), setin, sep = ": "), collapse = "\n"))
message("**End settings**")

stopifnot(file.exists(path_ufid_lib))

# connect to databases
source("~/connect-ntsp.R")
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_lib)

# Will hang if index is not present
log_info("{elastic::count(escon, index)} docs in index")

log_info("Add rtt field to index pattern")
ntsportal::es_add_rtt(escon, config::get("index_pattern"))

log_info("Add rt_cluster field")
ntsportal::es_add_rt_cluster(escon, config::get("index_pattern"))

stopifnot(es_check_docs_fields(escon, index))

# check for duplicates
if (!es_no_duplicates(escon, index))
  stop("Duplicate features found")

stopifnot(DBI::dbIsValid(udb))
stopifnot("feature" %in% DBI::dbListTables(udb))
log_info("Steps included: {paste(STEPS, collapse = ', ')}.")

if (1 %in% STEPS) {
  # processing pos ####
  log_info("Starting pos processing")
  log_info("initial pass through ufid-db")
  tryCatch(
    es_assign_ufids(escon, udb, index, "pos"),
    error = function(cnd) {
      log_error("Error in step 1")
      message("error text: ", cnd)
    }
  )
} 

# Clustering to look for new ufids
if (2 %in% STEPS) {
  log_info("Looking for new clusters, +ESI")
  successNewUfid <- FALSE
  while (!successNewUfid) {
    tryCatch(
      successNewUfid <- ubd_new_ufid(udb, escon, index, "pos"),
      error = function(cnd) {
        log_error("Error in step 2, udb_new_ufid")
        log_info("Error text: {conditionMessage(cnd)}")
        log_info("Taking a 1 h break")
        Sys.sleep(3600)
        log_info("Repeating")
        successNewUfid <<- FALSE
      }
    )
  }

  if (successNewUfid) {
    # sometimes we see a disconnection of DB, so just connect again
    udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_lib)
    log_info("no more features found to assign")
    log_info("final pass through ufid-db")
    # Pass through all ufids one more time for good measure
    tryCatch(
      es_assign_ufids(escon, udb, index, "pos"),
      error = function(cnd) {
        log_error("Error in step 2, second pass through ufid-lib")
        log_info("Error text: {conditionMessage(cnd)}")
      }
    )
  }
} 

log_info("End of pos processing")

# processing neg ####
log_info("Starting neg processing")

if (3 %in% STEPS) {
  log_info("initial pass through ufid-db")
  udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_lib)
  
  tryCatch(
    es_assign_ufids(escon, udb, index, "neg"),
    error = function(cnd) {
      log_error("Error in step 3")
      message("error text: ", cnd)
    }
  )
} 

if (4 %in% STEPS) {
  log_info("looking for new clusters, -ESI")
  success <- FALSE
  tryCatch(
    success <- ubd_new_ufid(udb, escon, index, "neg"),
    error = function(cnd) {
      log_error("Error in step 4")
      message("error text: ", cnd)
    }
  )
  
  if (success) {
    udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_lib)
    
    # pass through all ufids one more time for good measure
    log_info("2nd pass through ufid-db, -ESI")
    tryCatch(
      es_assign_ufids(escon, udb, index, "neg"),
      error = function(cnd) {
        log_error("Error in step 4, 2nd pass through ufid-lib")
        message("error text: ", cnd)
      }
    )
  }
} 

log_info("Ended neg processing")

DBI::dbDisconnect(udb)

log_info("Disconnected DB")
endTime <- lubridate::now()
hrs <- round(as.numeric(endTime - startTime, units = "hours"))

log_info("----------- Completed assign-ufids-ntsp.R in {hrs} h -----------")
