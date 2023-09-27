
#' @export
dist_make_parallel <- function(x, distance_fcn, numCores, ...) 
{ 
  #browser()
  distance_from_idxs <- function(idxs) {
    i1 <- idxs[1]
    i2 <- idxs[2]
    distance_fcn(x[i1, ], x[i2, ], ...)
  }
  size <- nrow(x)
  clf <- parallel::makeForkCluster(numCores)
  comb <- RcppAlgos::comboGeneral(size, m = 2)
  d <- parallel::parApply(clf, comb, 2, distance_from_idxs)
  parallel::stopCluster(clf)
  attr(d, "Size") <- size
  xnames <- rownames(x)
  if (!is.null(xnames)) {
    attr(d, "Labels") <- xnames
  }
  attr(d, "Diag") <- FALSE
  attr(d, "Upper") <- FALSE
  class(d) <- "dist"
  d
}

dist_make_parallel_temp <- function(x, distance_fcn, numCores, comb, ...) 
{ 
  #browser()
  distance_from_idxs <- function(idxs) {
    i1 <- idxs[1]
    i2 <- idxs[2]
    distance_fcn(x[i1, ], x[i2, ], ...)
  }
  size <- nrow(x)
  clf <- parallel::makeForkCluster(numCores)
  # passed comb externally because not able to compile RcppAlgos on Centos
  d <- parallel::parApply(clf, comb, 2, distance_from_idxs)
  parallel::stopCluster(clf)
  attr(d, "Size") <- size
  xnames <- rownames(x)
  if (!is.null(xnames)) {
    attr(d, "Labels") <- xnames
  }
  attr(d, "Diag") <- FALSE
  attr(d, "Upper") <- FALSE
  class(d) <- "dist"
  d
}

distdex <- function(i, j, n) #given row, column, and n, return index
  n*(i-1) - i*(i-1)/2 + j-i

rowcol <- function(ix, n) { #given index, return row and column
  nr <- ceiling(n-(1+sqrt(1+4*(n^2-n-2*ix)))/2)
  nc <- n-(2*n-nr+1)*nr/2+ix+nr
  c(nr,nc)
}


tconvert <- function(unixtime) {
  as.POSIXct(unixtime, origin = "1970-01-01 00:00")
}

#' @export
test_config <- function() {
  message(config::get("mztol_mda"))
}


#' Check if there are duplicates in the es database
#'
#' A cardinality estimation is made. If the number of different docs
#' (based on mz, rt, intensity, pol, start and station) and the
#' number of docs is the same, it is assumed no duplicates. The estimation
#' is done within 5% accuracy since the cardinality computation is fuzzy
#' (HyperLogLog++ algorithm). The question here is, is it worth it.
#'
#' @param escon
#' @param index
#'
#' @return TRUE if no duplicates found.
#' @export
#'
es_no_duplicates <- function(escon, index) {

  # total docs
  totDocs <- elastic::count(escon, index)

  # cardinality using maximum available precision
  resp <- elastic::Search(
    escon, index,
    body = '
    {
  "runtime_mappings": {
    "combi": {
      "type": "keyword",
      "script": "emit(doc[\'intensity\'].value + \' \' + doc[\'station\'].value + \' \' + doc[\'mz\'].value + \' \' + doc[\'rt\'].value + \' \' + doc[\'pol\'].value + \' \' + doc[\'start\'].value)"
    }
  },
  "size": 0,
  "aggs": {
    "num_different": {
      "cardinality": {
        "field": "combi",
        "precision_threshold": 40000
      }
    }
  }
}
    ')
  numDiff <- resp$aggregations$num_different$value
  # cardinality calculation is fuzzy, assume at least 5% accuracy
  return(abs(totDocs - numDiff) / totDocs < 0.05)
}


#' Check for consistency of documents in an ntsp index.
#' 
#' In order for alignment to work, all features must have at least mz and the retention time of the
#' bfg method (bfg_nts_rp1) in the rtt table, either experimental or predicted. They must also have
#' the rt_clustering field which copies this rt to the top level 
#'
#' @param escon Connection to ntsp
#' @param index Index name
#' @param methodName Name of method which must be provided
#'
#' @return FALSE if not all consistency checks successful. TRUE otherwise.
#' @export
#'
#' @examples
es_check_docs_fields <- function(escon, index, methodName = "bfg_nts_rp1") {
  # check that start is present
  res <- elastic::Search(escon, index, size = 0, body = '
{
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "start"
          }
        }
      ]
    }
  }
}
')
  checkStart <- res$hits$total$value
  if (checkStart > 0) {
    logger::log_fatal("Found {checkStart} docs without start")
    return(FALSE)
  }
    
  
  # check that mz is present
  res2 <- elastic::Search(escon, index, size = 0, body = '
{
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "mz"
          }
        }
      ]
    }
  }
}
')
  checkMz <- res2$hits$total$value
  if (checkMz > 0) {
    logger::log_fatal("Found {checkMz} docs without mz")
    return(FALSE)
  }
    
  
  # check that station is present
  res3 <- elastic::Search(escon, index, size = 0, body = '
{
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "station"
          }
        }
      ]
    }
  }
}
')
  checkSta <- res3$hits$total$value
  if (checkSta > 0) {
    logger::log_fatal("Found {checkSta} docs without station")
    return(FALSE)
  }
  
  
  
  # check that rtt is present with the right method
  totDocs <- elastic::Search(escon, index, size = 0, body = '
                            {
  "query": {
    "match_all": {}
  }
}
                            ')$hits$total$value
  
  stopifnot(is.numeric(totDocs), length(totDocs) == 1)
  
  docsWithMethod <- elastic::Search(escon, index, size = 0, body = sprintf('
                                    {
  "query": {
    "nested": {
      "path": "rtt",
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "rtt.method": {
                  "value": "%s"
                }
              }
            }
          ]
        }
      }
    }
  }
}
                            ', methodName))$hits$total$value
  if (docsWithMethod != totDocs) {
    logger::log_fatal("Found {totDocs - docsWithMethod} docs without an rtt entry for {methodName}")
    return(FALSE)
  }
  
  # check for rt_clustering
  res4 <- elastic::Search(escon, index, size = 0, body = '
{
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "rt_clustering"
          }
        }
      ]
    }
  }
}
')
  checkRtC <- res4$hits$total$value
  if (checkRtC > 0) {
    logger::log_fatal("Found {checkRtC} docs without rt_clustering")
    return(FALSE)
  }
  
  
  logger::log_info("Index {index} doc consistency checks complete.")
  return(TRUE)
}


#' Plot an ms2 spectrum for viewing
#'
#' @param ms2Spec data.frame with columns mz and int
#'
#' @return
#' @export
#'
#' @examples
#' @import ggplot2
plot_ms2 <- function(ms2Spec) {
  # change name of int field
  if (!is.element("int", colnames(ms2Spec))) {
    colnames(ms2Spec)[grep("int", colnames(ms2Spec))] <- "int"
  }
  ggplot(ms2Spec, aes(mz, int, label = round(mz,4))) +
    geom_segment(aes(x = mz, xend = mz, y = 0, yend = int),
                 stat = "identity", linewidth = .5, alpha = .5) +
    theme_bw(base_size = 14) +
    geom_text(data = ms2Spec[ms2Spec$int > 0.01, ], check_overlap = TRUE, vjust = -0.5) +
    scale_y_continuous(expand = c(0, 0)) +
    coord_cartesian(ylim = c(0, max(ms2Spec$int)*1.1), xlim = c(0, max(ms2Spec$mz) + 5)) +
    ylab("Intensity") +
    xlab("m/z (u)")
}


#' Create ufid-library 
#'
#' Produce an empty ufid-library (SQLite database file) at a specified location.
#'
#' @param pth Path to the where the SQLite file should be saved
#'
#' @return Prints the list of tables and returns full path to library
#' @export
#' @import DBI
create_ufid_lib <- function(pth) {
  
  fdb <- dbConnect(RSQLite::SQLite(), pth)
  
  dbExecute(fdb, "PRAGMA foreign_keys = ON;")
  
  dbExecute(fdb, "CREATE TABLE feature (
  ufid INTEGER PRIMARY KEY,
  mz REAL NOT NULL,
  isotopologue TEXT,
  adduct TEXT,
  compound_name TEXT,
  cas TEXT,
  smiles TEXT,
  inchi TEXT,
  formula TEXT,
  comment TEXT,
  polarity TEXT NOT NULL,
  date_added INTEGER NOT NULL
  );")
  
  dbExecute(fdb, "CREATE TABLE tag (
          tag_id INTEGER PRIMARY KEY,
          tag_text TEXT NOT NULL UNIQUE
          );")
  
  dbExecute(fdb, "CREATE TABLE feature_tag (
  tag_id INTEGER,
  ufid INTEGER,
  PRIMARY KEY (tag_id, ufid),
  FOREIGN KEY (ufid)
    REFERENCES feature (ufid)
      ON DELETE CASCADE
      ON UPDATE CASCADE,
  FOREIGN KEY (tag_id)
    REFERENCES tag (tag_id)
      ON DELETE CASCADE
      ON UPDATE CASCADE
  );")
  
  dbExecute(fdb, "CREATE TABLE retention_time (
          method TEXT NOT NULL,
          rt REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
  );")
  
  dbExecute(fdb, "CREATE TABLE ms1 (
          method TEXT NOT NULL,
          mz REAL NOT NULL,
          rel_int REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
  );")
  
  dbExecute(fdb, "CREATE TABLE ms2 (
          method TEXT NOT NULL,
          mz REAL NOT NULL,
          rel_int REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
  );")
  
  dbListTables(fdb)
  
  dbDisconnect(fdb)
  
  if (file.exists(pth))
    normalizePath(pth) else NULL 
} 





#' Remove all ufid from NTSPortal-DB and a ufid library
#'
#' @param escon elastic::connect connection object
#' @param es_index index name or pattern
#' @param ufidLibPath path to ufid-lib
#'
#' @return TRUE if completed
#' @export
#' @import dplyr
reset_alignment <- function(escon, es_index, ufidLibPath) {
  
  elastic::docs_update_by_query(escon, es_index, refresh = "true", body =
    '
    {
      "query": {
        "exists": {
          "field": "ufid"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ufid\')",
        "lang": "painless"
      }
    }
    '
  )
  Sys.sleep(5)
  elastic::docs_update_by_query(escon, es_index, refresh = "true", body =
    '
    {
      "query": {
        "exists": {
          "field": "ucid"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ucid\')",
        "lang": "painless"
      }
    }
    '
  )
  Sys.sleep(5)
  elastic::docs_update_by_query(escon, es_index, refresh = "true", body =
                                  '
    {
      "query": {
        "exists": {
          "field": "ufid"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ufid2\')",
        "lang": "painless"
      }
    }
    '
  )
  Sys.sleep(5)
  elastic::docs_update_by_query(escon, es_index, refresh = "true", body =
    '
    {
      "query": {
        "exists": {
          "field": "ucid"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ucid2\')",
        "lang": "painless"
      }
    }
    '
  )
  udb <- DBI::dbConnect(RSQLite::SQLite(), ufidLibPath)
  
  DBI::dbExecute(udb, "PRAGMA foreign_keys = ON;")
  
  DBI::dbExecute(udb, "DELETE FROM feature;")
  nr <- c(
    tbl(udb, "feature") %>% collect() %>% nrow(),
    tbl(udb, "retention_time") %>% collect() %>% nrow(),
    tbl(udb, "ms1") %>% collect() %>% nrow(),
    tbl(udb, "ms2") %>% collect() %>% nrow()
  )
  if (any(nr != 0))
    warning("Some tables still have rows left over")
  DBI::dbDisconnect(udb)
  invisible(TRUE)
}


#' Handle errors from ElasticSearch
#' 
#' Errors from ElasticSearch are returned by package 'elastic' as text. This
#' function will take the error condition and take appropriate action.
#'
#' @param thisCnd Error condition thrown in the `tryCatch` context
#'
#' @return No return value
#' @export
es_error_handler <- function(thisCnd) {
  if (is.character(conditionMessage(thisCnd)) && 
      grepl("429", conditionMessage(thisCnd))) {
    logger::log_warn("Error 429 from ElasticSearch, taking a 1 h break")
    Sys.sleep(3600)
  }
}


#' Return a new, unique ufid number
#'
#' The next ufid number is returned, for creating a new cluster. Will double
#' check that this number is not already present.
#' 
#' @param udb 
#' @param escon 
#' @param index 
#'
#' @return length one integer
#' @import dplyr
#' @export
#'
get_next_ufid <- function(udb, escon, index) {
  allUfid <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid
  newUfid <- if (length(allUfid) == 0)
    1L else min(which(!(1:(max(allUfid)+1) %in% allUfid)))
  newUfid <- as.integer(newUfid)
  # Double check that this ufid is not found in the ntsp
  checkUf <- elastic::Search(escon, index, body = sprintf(
    '
            {
              "query": {
                "term": {
                  "ufid": {
                    "value": %i
                  }
                }
              },
              "size": 0
            }
            ', newUfid
  ))
  if (checkUf$hits$total$value != 0) {
    logger::log_error("Creating new ufid number failed, already found in ntsp")
    stop("Mismatch between ntsp and ufid lib in ufid ", newUfid)
  }
  checkUf2 <- tbl(udb, "feature") %>% filter(ufid == newUfid) %>% collect()
  if (nrow(checkUf2) != 0) {
    logger::log_error("Creating new ufid number failed, already found in udb")
    stop("Ufid lib already contains ", newUfid)
  }
  newUfid
}

#' Connect to a sqlite db (ufid-library)
#'
#' @param pth 
#'
#' @return connection object
#' @export
conn_udb <- function(pth) {
  DBI::dbConnect(RSQLite::SQLite(), pth)
}

# TODO general function for adding a value to es
# what to do with numbers and logicals?
es_add_value <- function(escon, index, esid, fieldName, value) {
  
  elastic::docs_update(escon, index, esid, body = sprintf('
                                                          {
  "script": {
    "source": "ctx._source.%s = \'%s\'",
    "lang": "painless"
    }
  }    
  ', fieldName, value))
}  
  
#' Return the number of Gb of free memory on linux server
#'
#' @return numeric length 1
#' @export
#'
free_gb <- function() {
  x <- system("free -g", intern = T)
  x <- strsplit(x[2], "\\s+")
  as.numeric(x[[1]][length(x[[1]])])
}  