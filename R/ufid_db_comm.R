


#' Remove a ufid from the ufid library
#'
#' @param udb 
#' @param ufid 
#'
#' @return
#' @export
#'
udb_remove_ufid <- function(udb, ufid) {
  DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
  x <- DBI::dbExecute(udb, sprintf("DELETE FROM feature WHERE ufid == %i;", ufid))
  logger::log_info("Removed {x} rows from ufid lib")
  invisible(TRUE)
}

#' Clean spectra within ufid db
#'
#' @param udb
#' @param ufid
#' @param msLevel
#'
#' @return
#' @import dplyr
#' @export
udb_clean_spectra <- function(udb, ufid, msLevel) {

  mz_ <- tbl(udb, "feature") %>% filter(ufid == !!ufid) %>% select(mz) %>%
    collect() %>% .$mz

  spec_ <- tbl(udb, msLevel) %>% filter(ufid == !!ufid) %>% collect()

  stopifnot(all(spec_$method == "bfg_nts_rp1"))

  spec_ <- subset(spec_, , c(mz, rel_int))

  mztol_ <- ifelse(msLevel == "ms1", 0.005, 0.015)
  spec_clean <- ntsportal::clean_spectrum(spec_, mztol = mztol_, msLevel = msLevel,
                               precursorMz = mz_)
  # delete old spectra
  DBI::dbExecute(udb, sprintf('
               DELETE FROM %s
               WHERE ufid == %i;
               ', msLevel, ufid))

  spec_clean$ufid <- ufid
  spec_clean$method <- "bfg_nts_rp1"

  changes <- DBI::dbAppendTable(udb, msLevel, spec_clean)
  message(changes, " rows were changed")
  TRUE
}


#' Add feature to ufid db
#'
#' if this is not found in the database
#' for now only for the case where ms1 and ms2 are present
#'
#' @param feat
#' @param udb
#'
#' @return
#' @import dplyr
#' @export
udb_add_feature <- function(feat, udb) {

  feat <- validate_feature(feat)

  # get next ufid

  newUfid <- max(tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid) + 1L

  stopifnot(length(newUfid) == 1, is.integer(newUfid))

  # create new row for feature table
  tbl(udb, "feature") %>% collect()
  tbl(udb, "retention_time") %>% collect()
  tbl(udb, "ms1") %>% collect()
  tbl(udb, "ms2") %>% collect()
  newFeatRow <- data.frame(ufid = newUfid, mz = feat$mz, isotopologue = NA, adduct = NA,
                           compound_name = NA, cas = NA, smiles = NA, inchi = NA,
                           formula = NA, comment = NA, polarity = feat$pol,
                           date_added = as.numeric(Sys.time()))

  DBI::dbAppendTable(udb, "feature", newFeatRow)

  # new row retention_time
  newRtRow <- data.frame(method = "bfg_nts_rp1", rt = feat$rt, ufid = newUfid)
  DBI::dbAppendTable(udb, "retention_time", newRtRow)

  # new ms1
  newMs1 <- norm_ms1(feat$ms1, feat$mz)
  newMs1 <- clean_spectrum(newMs1, precursorMz = feat$mz)  # MS1 spectra have multiple entries
  newMs1Rows <- data.frame(method = "bfg_nts_rp1", mz = newMs1$mz,
                           rel_int = newMs1$int, ufid = newUfid)
  DBI::dbAppendTable(udb, "ms1", newMs1Rows)

  # new ms2
  newMs2 <- norm_ms2(feat$ms2)
  newMs2Rows <- data.frame(method = "bfg_nts_rp1", mz = newMs2$mz,
                           rel_int = newMs2$int, ufid = newUfid)
  DBI::dbAppendTable(udb, "ms2", newMs2Rows)

  message("added ufid: ", newUfid)

  newUfid
}


#' Update the entire ufid library
#'
#' @param udb ufid-library connection object produced with DBI::dbConnect
#' @param escon 
#' @param index 
#'
#' @return
#' @export
#'
udb_update_all <- function(udb, escon, index) {
  allUfs <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% unlist()
  logger::log_info("Updating entire ufid library with {length(allUfs)} ufids")
  startTime <- lubridate::now()
  for (u in allUfs) {
    udb_update(udb = udb, escon = escon, index = index, ufid_to_update = u)
  }
  endTime <- lubridate::now()
  
  hrs <- round(as.numeric(endTime - startTime, units = "hours"))
  
  logger::log_info("Completed updating entire ufid library in {hrs} h")
}


#' Update ufid db based on average aggregated results from ntsportal
#'
#' The ufid database is updated at the end of a session to
#' improve it based on statistical averaging of previous data.
#' Caution is needed here that incorrect data does not contaminate the "gold
#' standard"
#' Index to be used should be index-pattern to include all available indexes.
#'
#' @param udb ufid-library connection object produced with DBI::dbConnect
#' @param escon
#' @param index
#' @param ufid
#'
#' @return
#' @export
#'
#' @examples
#' @import dplyr
udb_update <- function(udb, escon, index, ufid_to_update) {
  
  stopifnot(is.integer(ufid_to_update), !is.na(ufid_to_update))
  
  # get ufid polarity
  
  polres <- elastic::Search(escon, index, body = sprintf('
    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "size": 0,
      "aggs": {
        "polarity": {
          "terms": {
            "field": "pol",
            "size": 2
          }
        }
      }
    }
    ', ufid_to_update))
  polarity_ <- vapply(polres$aggregations$polarity$buckets, function(x) x$key, character(1))
  stopifnot(length(polarity_) == 1)
  
  # update mz
  
  # get average mz from es
  res <- elastic::Search(escon, index, body = sprintf(
    '
    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "aggs": {
        "av_mz": {
          "avg": {
            "field": "mz"
          }
        }
      },
      "size": 0
    }
    ', ufid_to_update)
  )
  mz_es <- round(res$aggregations$av_mz$value, 4)
  stopifnot(is.double(mz_es))
  
  # replace mz in feature table
  DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
  DBI::dbExecute(udb, "PRAGMA cache_size = -10000000;")  # change cache to 10 GB
  # if ufid not present in the db, add it first
  notFound <- tbl(udb, "feature") %>% filter(ufid == !!ufid_to_update) %>% 
    select(ufid) %>% collect() %>% nrow() == 0
  if (notFound) {
    newFeatRow <- data.frame(ufid = ufid_to_update, mz = mz_es, isotopologue = NA, adduct = NA,
                             compound_name = NA, cas = NA, smiles = NA, inchi = NA,
                             formula = NA, comment = NA, polarity = polarity_,
                             date_added = as.numeric(Sys.time()))
    DBI::dbAppendTable(udb, "feature", newFeatRow)
  }
  
  resu <- DBI::dbExecute(udb, sprintf(
    "
    UPDATE feature
    SET mz = %.4f
    WHERE ufid = %i;
    ", mz_es, ufid_to_update))
  # res should be 1 (1 change made)
  stopifnot(resu == 1)
  
  # get average rt from db
  # rt_clustering field is used since this has been set to bfg_nts_rp1 anyway and this
  # is the method currently used for clustering
  # Both experimental and predicted retention times are therefore used to build the 
  # average. Originally the idea was to only use experimental RTs, but what about
  # ufids only found in external data? Maybe there needs to be a query and use 
  # predicted RTs only when experimental RTs are not available... (not done yet)
  
  res2 <- elastic::Search(escon, index, body = sprintf(
    '
    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "aggs": {
        "art": {
          "avg": {
            "field": "rt_clustering"
          }
        }
      },
      "size": 0
    }
    ', ufid_to_update)
  )
  
  rt_es <- res2$aggregations$art$value
  stopifnot(is.double(rt_es), length(rt_es) == 1, !is.na(rt_es))
  
  # if ufid does not exist, add it first
  notFound2 <- tbl(udb, "retention_time") %>% 
    filter(ufid == !!ufid_to_update) %>% select(ufid) %>% 
    collect() %>% nrow() == 0
  if (notFound2) {
    newRtRow <- data.frame(method = "bfg_nts_rp1", rt = rt_es, ufid = ufid_to_update)
    DBI::dbAppendTable(udb, "retention_time", newRtRow)
  }
  
  # Update value in ufid db
  resu2 <- DBI::dbExecute(udb,
                          sprintf('
          UPDATE retention_time
          SET rt = %.2f
          WHERE
             method = "bfg_nts_rp1" AND ufid = %i;
          ', rt_es, ufid_to_update))
  stopifnot(resu2 == 1)
  
  # Update MS1/MS2
  get_combined_spectrum <- function(msLevel, ufid_, mztol_combine, mz_precursor) {
    stopifnot(is.character(msLevel), length(msLevel) == 1,
              msLevel %in% c("ms1", "ms2"))
    # get all available spectra as a list
    # This will get all data regardless of the source
    # This is considered the simpler way of doing this rather than choosing the source depending
    # on what is available
    esSpecTemp <- elastic::Search(escon, index, body = sprintf(
      '
      {
        "query": {
          "bool": {
            "filter": [
              {
                "term": {
                  "ufid": %i
                }
              },
              {
                "nested": {
                  "path": "%s",
                  "query": {
                    "exists": {
                      "field": "%s.mz"
                    }
                  }
                }
              }
            ]
          }
        },
        "_source": "%s",
        "size": 200
      }
    ', ufid_, msLevel, msLevel, msLevel)
    )
    hits <- esSpecTemp$hits$hits
    esSpecs <- lapply(hits, function(x) {
      specs <- x[["_source"]][[msLevel]]
      specs <- lapply(specs, as.numeric)
      do.call("rbind", specs)
    })
    if (all(sapply(esSpecs, is.null)))
      return(NULL)
    esSpecs <- Filter(Negate(is.null), esSpecs)
    # combine list into one additive spectrum
    esSpecAll <- as.data.frame(do.call("rbind", esSpecs))
    colnames(esSpecAll) <- c("mz", "int")
    sp <- ntsportal::clean_spectrum(esSpecAll, mztol = mztol_combine, msLevel = msLevel,
                                    precursorMz = mz_precursor)
    sp
  }
  
  udb_update_spectrum <- function(msLevel, method_, spec_, mz_, ufid_) {
    stopifnot(is.character(msLevel), length(msLevel) == 1,
              msLevel %in% c("ms1", "ms2"))
    stopifnot(is.data.frame(spec_), nrow(spec_) > 0)
    
    # delete rows of old spectrum (if they exist)
    rowsExist <- tbl(udb, msLevel) %>% 
      filter(ufid == !!ufid_ & method == !!method_) %>% 
      slice_min(mz, n = 1) %>% collect() %>% nrow() > 0
    if (rowsExist) {
      resu3 <- DBI::dbExecute(udb,
                              sprintf('
        DELETE FROM %s
        WHERE method = "%s" AND ufid = %i;
                ', msLevel, method_, ufid_))
      stopifnot(resu3 > 0)
    }
    
    # add new rows
    spec_$method <- method_
    spec_$ufid <- ufid_
    colnames(spec_) <- sub("int", "rel_int", colnames(spec_))
    resu4 <- DBI::dbAppendTable(udb, msLevel, spec_)
    stopifnot(resu4 > 0)
    TRUE
  }
  
  # get new averaged MS1 spectrum
  ms1spec <- get_combined_spectrum("ms1", ufid_to_update, mztol_combine = 0.01, mz_precursor = mz_es)
  # get new averaged MS2 spectrum
  ms2spec <- get_combined_spectrum("ms2", ufid_to_update, mztol_combine = 0.03, mz_precursor = mz_es)
  
  # update ufid with new MS1 spectrum
  if (!is.null(ms1spec))
    success1 <- udb_update_spectrum("ms1", "bfg_nts_rp1", ms1spec, mz_es, ufid_to_update)
  
  
  # update ufid with new MS2 spectrum
  if (!is.null(ms2spec))
    success2 <- udb_update_spectrum("ms2", "bfg_nts_rp1", ms2spec, mz_es, ufid_to_update)
  
  
  invisible(TRUE)
}

udb_connect <- function(pth) {
  DBI::dbConnect(RSQLite::SQLite(), pth)
}
