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

# Integrity checks ####

check_dbas_replicate_regex <- function(escon, rfindex) {
  
  # dbas_replicate_regex must have brackets
  ask_es <- function(rgx) {
    elastic::Search(
      escon, rfindex, size = 10000, source ="dbas_replicate_regex", 
      body = list(
        query = list(
          bool = list(
            must = list(
              list(
                exists = list(
                  field = "dbas_replicate_regex"
                )
              )
            ),
            must_not = list(
              list(
                regexp = list(
                  dbas_replicate_regex = rgx
                )
              )
            )
          )
        )
      )
    )
  }
  res1 <- ask_es(".*\\(.*")
  res2 <- ask_es(".*\\).*")
  addUp <- sum(c(res1$hits$total$value, res2$hits$total$value))
  if (addUp > 0) {
    bad <- paste(
      c(
        unique(res_field(res1, "dbas_replicate_regex")),
        unique(res_field(res2, "dbas_replicate_regex"))
      ),
      collapse = "\n"
    )
    stop("There are dbas_replicate_regex which are missing '()':\n", bad)
  }
  
  # For each entry, check that the regex and filename work with str_replace
  # Which is how the processing groups the replicates together
  res3 <- ntsportal::es_search_paged(
    escon, rfindex, source = c("filename", "dbas_replicate_regex"),
    sort = "filename",
    searchBody = list(
      query = list(
        exists = list(
          field = "dbas_replicate_regex"
        )
      )
    )
  )
  df <- data.frame(
    rgx = res_field(res3, "dbas_replicate_regex"),
    fn = res_field(res3, "filename")
  )
  df$repn <- apply(df, 1, function(x) stringr::str_replace(x[2], x[1], "\\1"))
  df$bad <- df$fn == df$repn
  
  if (sum(df$bad > 0)) {
    message("There are dbas_replicate_regex entries which do not match the filename")
    print(subset(df, bad, c("rgx", "fn")))
    stop("Correct errors before continuing")
  }
  perGroup <- as.numeric(by(df, df$repn, nrow, simplify = T))
  if (any(perGroup < 2)) {
    b <- df[df$repn %in% names(which(by(df, df$repn, nrow, simplify = T) < 2)), "fn"]
    stop("These files do not produce replicate groups\n", 
         paste(b, collapse = "\n"))
  }
  
  invisible(TRUE)
} 


check_dbas_blank_regex <- function(escon, rfindex) {
  
  res <- elastic::Search(
    escon, rfindex, source = c("dbas_blank_regex", "filename", "path", "blank"),
    size = 10000, asdf = T,
    body = list(
      query = list(
        term = list(
          blank = T
        )
      )
    )
  )
  
  df <- res$hits$hits
  
  patFound <- mapply(
    function(pat, fn)  grepl(pat, fn),
    df$`_source.dbas_blank_regex`, df$`_source.filename`
  )
  if (!all(patFound)) {
    pths <- paste(df[!patFound, "_source.path"], collapse = "\n")
    logger::log_error("Value of the field dbas_blank_regex not found in the filename of the following files:\n{pths}")
    build_es_query_for_ids(
      ids = df[!patFound, "_id"], 
      toShow = c("blank", "dbas_blank_regex", "filename")
    )
    stop("check_dbas_blank_regex failed")
  }
  invisible(T)
}




#' Check consistency of msrawfiles DB
#' 
#' Must be done before any processing operation
#'
#' @param escon 
#' @param rfindex
#' @param locationRf Root directory for all rawfiles
#'
#' @return TRUE if successful (invisibly)
#' @import logger
#' @export
#'
check_integrity_msrawfiles <- function(escon, rfindex, locationRf) {
  
  locationRf <- normalizePath(locationRf)
  
  # Check presence of spectral library
  resp2 <- elastic::Search(escon, rfindex, body = '
                {
  "query": {
    "match_all": {}
  },
  "aggs": {
    "csl_loc": {
      "terms": {
        "field": "dbas_spectral_library",
        "size": 100
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
  
  # Check presence of IS tables
  resp3 <- elastic::Search(escon, rfindex, body = '
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
  
  # Check that certain fields exist in all docs
  stopifnot(
    check_field(escon, rfindex, "dbas_is_table"),
    check_field(escon, rfindex, "dbas_area_threshold"),
    check_field(escon, rfindex, "dbas_rttolm"),
    check_field(escon, rfindex, "dbas_mztolu"),
    check_field(escon, rfindex, "dbas_mztolu_fine"),
    check_field(escon, rfindex, "dbas_ndp_threshold"),
    check_field(escon, rfindex, "dbas_rtTolReinteg"),
    check_field(escon, rfindex, "date_measurement"),
    check_field(escon, rfindex, "dbas_ndp_m"),
    check_field(escon, rfindex, "dbas_ndp_n"),
    check_field(escon, rfindex, "dbas_instr"),
    check_field(escon, rfindex, "pol"),
    check_field(escon, rfindex, "chrom_method"),
    check_field(escon, rfindex, "matrix"),
    check_field(escon, rfindex, "data_source"),
    check_field(escon, rfindex, "dbas_index_name"),
    check_field(escon, rfindex, "blank"),
    check_field(escon, rfindex, "path"),
    check_field(escon, rfindex, "sample_source"),
    check_field(escon, rfindex, "duration", onlyNonBlank = TRUE)
  )
  
  # Check that blank files do not have coordinates
  checkLoc <- elastic::Search(escon, rfindex, body = '
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
  resp <- es_search_paged(escon, rfindex, searchBody = '
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
    isBlank <- elastic::docs_mget(
      escon, rfindex, ids = idsDup, source = "blank", verbose = FALSE
    )$docs
    if (all(sapply(isBlank, function(d) d[["_source"]][["blank"]]))) {
      log_warn("There are duplicated filenames in the db (but only for blanks)")
    } else {
      stop("There are duplicated filenames in the db")  
    }
  }
  
  # Check that there are no mismatches between IS table and polarity
  polcheck_body <- function(pols) {
    sprintf('
  {
    "query": {
      "bool": {
        "must": [
          {
            "regexp": {
              "dbas_is_table": ".*%s.*"
            }
          },
          {
            "regexp": {
              "filename": ".*%s.*"
            }
          },
          {
            "term": {
              "pol": "%s"
            }
          }
        ]
      }
    }
  }             
  ', pols[1], pols[2], pols[3])
  }
  check_polarity_consitency <- function(pols) {
    elastic::Search(escon, rfindex, body = polcheck_body(pols))$hits$total$value == 0
  }
  
  # Get all permutations of 3 polarities
  perms <- gtools::permutations(2, 3, c("pos", "neg"), repeats.allowed = T)[2:7,]
  polCheck <- apply(perms, 1, check_polarity_consitency)
  if (any(!polCheck)) {
    stop("Inconsistencies in the polarity of filename, pol and dbas_is_table,
         use this query to find them:", 
         apply(perms[!polCheck, , drop = F], 1, polcheck_body))
  }
  
  # Check that all files are located in correct location. If not, give a warning.
  resm <- elastic::Search(escon, rfindex, source = "path", size = 10000, body = sprintf('
    {
      "query": {
        "bool": {
          "must_not": [
            {
              "regexp": {
                "path": "%s.*"
              }
            }
          ]
        }
      }
    }
  ', locationRf))
  if (resm$hits$total$value > 0) {
    badp <- vapply(resm$hits$hits, function(x) x[["_source"]]$path, character(1))
    tx <- paste(unique(dirname(badp)), collapse = "\n")
    log_warn("{length(badp)} files are not saved in HRMS/Messdaten, see directories:\n {tx}")
  }
  
  # Check that 
  check_dbas_replicate_regex(escon, rfindex)
  check_dbas_blank_regex(escon, rfindex)
  
  invisible(TRUE)
}

#' Check that evaluation batches have consistent settings
#' 
#' Evaluation is run in batches and certain settings must be the same
#' for all files in the batch.
#'
#' @param escon 
#' @param rfindex 
#' @param batches a list of batches, where each batch is character vector of 
#' elasticsearch document ids
#'
#' @return TRUE if successful
#' @export
#'
check_batches_eval <- function(escon, rfindex, batches) {
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
  
  allSame <- vapply(batches, function(x) {
    all(vapply(mustBeSame, check_uniformity, esids = x, escon = escon, 
               rfindex = rfindex, logical(1)))
  }, logical(1))
  
  
  mustBeSameNonBlanks <- c(
    "dbas_build_averages",
    "dbas_replicate_regex"
  )
  
  allSame2 <- vapply(batches, function(x) {
    all(vapply(mustBeSameNonBlanks, check_uniformity, esids = x, escon = escon, 
               rfindex = rfindex, onlyNonBlanks = T, logical(1)))
  }, logical(1))
  
  # Need to stop here because the next function will not work if there is
  # no consistency in dbas_blank_regex
  if (!all(allSame) || !all(allSame2))
    stop("Batch similarity checks have failed")
  
  # Check that each batch contains at least one blank
  okBlank <- vapply(batches, function(x) {
    br <- get_field(escon, rfindex, x, "dbas_blank_regex", justone = T)
    fn <- get_field(escon, rfindex, x, "filename")
    any(grepl(br, fn))
  }, logical(1))
  if (any(!okBlank)) {
    b <- paste(vapply(batches[!okBlank], "[", i = 1, character(1)), collapse = ", ")
    logger::log_error("Blanks not found in batch(s) starting with {b}")
    return(FALSE)
  }
  
  invisible(TRUE)
}

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
