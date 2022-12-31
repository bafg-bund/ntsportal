
# Process the entire index to assign ufids (level 1) to all features

# usage:
# Rscript assign-ufids-ntsp.R index_name
# e.g.
# Rscript assign-ufids-ntsp.R g2_nts_upb &> logdat.log

library(ntsportal)
library(logger)

VERSION <- "2022-11-15"
index <- commandArgs(TRUE)
path_ufid_db <- "~/sqlite_local/ufid1.sqlite"
config_path <- "~/config.yml"

# Check args
if (!is.character(index) || length(index) != 1 || !grepl("^g2_nts", index))
  stop("Incorrect index specification")

ec <- config::get("elastic_connect", file = config_path)

escon <- elastic::connect(
  host = 'elastic.dmz.bafg.de', 
  port = 443, user=ec$user, 
  pwd  = ec$pwd,
  transport_schema = "https"
)

log_info("----- assign-ufids-ntsp.R v{VERSION} -----")
log_info("Processing {index} with {path_ufid_db}")

# do some checks
if (escon$ping()$cluster_name != "bfg-elastic-cluster") {
  stop("Connection to es-db not established")
}

# Will hang if index is not present
log_info("{elastic::count(escon, index)} docs in index")

stopifnot(es_check_docs_fields(escon, index))
# check for duplicates
if (!es_no_duplicates(escon, index))
  stop("Duplicate features found")
stopifnot(file.exists(path_ufid_db))
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
stopifnot(DBI::dbIsValid(udb))
stopifnot("feature" %in% DBI::dbListTables(udb))

# processing pos ####
log_info("Starting pos processing.")

# test that config.yml is present (this is not the same config.yml used for ntsp password!)
# it must be located in the current working dir and have all the settings needed for alignment.
if (is.null(config::get("mztol_rough_mda")))
  stop("no config.yml with settings found")

log_info("initial pass through ufid-db")
tryCatch(
  es_assign_ufids(escon, udb, index, "pos"),
  error = function(e) {
    log_error(e)
    DBI::dbDisconnect(udb)
    log_info("Disconnected DB")
  }
)

log_info("looking for new clusters")
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
