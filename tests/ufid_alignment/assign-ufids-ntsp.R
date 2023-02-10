
# Process the entire index to assign ufids (level 1) to all features

# usage:
# Rscript assign-ufids-ntsp.R index_name
# e.g.
# nohup Rscript assign-ufids-ntsp.R g2_nts_bfg &> ~/log-files/ufid-alignment-$(date +%y%m%d).log &

# before you start you must add rtt field and rt_clustering field to all documents

library(ntsportal)
library(logger)
library(glue)

VERSION <- "2023-01-04"
path_ufid_db <- "~/sqlite_local/ufid1.sqlite"
PATH_ADD_RTT <- "~/projects/ntsportal/tests/add_data/add-rtt.R"
PATH_ADD_RT_CLUSTERING <- "~/projects/ntsportal/tests/add_data/add-rt_clustering-field.R"

index <- commandArgs(TRUE)
#index <- "g2_nts_bfg"
#index <- "g2_nts_lanuv"


# Check args
if (!is.character(index) || length(index) != 1 || !grepl("^g2_nts", index))
  stop("Incorrect index specification")

# check for settings
stopifnot(file.exists("config.yml"))
stopifnot(is.numeric(config::get("ms2_ndp_min_score")))

log_info("----- assign-ufids-ntsp.R v{VERSION} -----")
log_info("Processing {index} with {path_ufid_db}")

# test that config.yml is present (this is not the same config.yml used for ntsp password!)
# it must be located in the current working dir and have all the settings needed for alignment.
if (is.null(config::get("mztol_rough_mda")))
  stop("no config.yml with settings found")

# print settings
message("**Settings**")
setin <- yaml::read_yaml("config.yml")$default
message(paste(paste(names(setin), setin, sep = ": "), collapse = "\n"))

stopifnot(file.exists(path_ufid_db))

# connect to databases
source("~/connect-ntsp.R")
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

# Will hang if index is not present
log_info("{elastic::count(escon, index)} docs in index")

# add rtt field (looks at all g2_nts* indices)

system(glue("Rscript {PATH_ADD_RTT}"))

# add rt_cluster field to where ever this is missing

system(glue("Rscript {PATH_ADD_RT_CLUSTERING}"))

stopifnot(es_check_docs_fields(escon, index))

# check for duplicates
if (!es_no_duplicates(escon, index))
  stop("Duplicate features found")

stopifnot(DBI::dbIsValid(udb))
stopifnot("feature" %in% DBI::dbListTables(udb))

# processing pos ####
log_info("Starting pos processing.")


log_info("initial pass through ufid-db")
tryCatch(
  es_assign_ufids(escon, udb, index, "pos"),
  error = function(e) {
    log_error(e)
    DBI::dbDisconnect(udb)
    log_info("Disconnected DB")
  }
)

# Clustering to look for new ufids
success <- tryCatch(ubd_new_ufid(udb, escon, index, "pos"),
                   error = function(e) {
                     log_error(e)
                     DBI::dbDisconnect(udb)
                     log_info("Disconnected DB")
                   })
 
if (success) {
  # sometimes we see a disconnection of DB, so just connect again
  udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
  log_info("no more features found to assign")
  log_info("final pass through ufid-db")
  # pass through all ufids one more time for good measure
  tryCatch(es_assign_ufids(escon, udb, index, "pos"),
           error = function(e) {
             log_error(e)
             DBI::dbDisconnect(udb)
             log_info("Disconnected DB")
           })
}

log_info("Ended pos processing")

# processing neg ####
log_info("Starting neg processing")
log_info("initial pass through ufid-db")
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

tryCatch(
  es_assign_ufids(escon, udb, index, "neg"),
  error = function(e) {
    log_error(e)
    DBI::dbDisconnect(udb)
    log_info("Disconnected DB")
  }
)

log_info("looking for new clusters")
success <- tryCatch(
  ubd_new_ufid(udb, escon, index, "neg"),
  error = function(e) {
    log_error(e)
    DBI::dbDisconnect(udb)
    log_info("Disconnected DB")
  }
)

if (success) {
  udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
  log_info("no more features found to assign")
  
  # pass through all ufids one more time for good measure
  log_info("final pass through ufid-db")
  tryCatch(
    es_assign_ufids(escon, udb, index, "neg"),
    error = function(e) {
      log_error(e)
      DBI::dbDisconnect(udb)
      log_info("Disconnected DB")
    }
  )
}

log_info("Ended neg processing")

DBI::dbDisconnect(udb)

log_info("Disconnected DB")
