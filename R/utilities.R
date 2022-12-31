
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
#' @param escon Connection to ntsp
#' @param index Index name
#'
#' @return
#' @export
#'
#' @examples
es_check_docs_fields <- function(escon, index) {
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
  checkSta <- res2$hits$total$value
  if (checkSta > 0) {
    logger::log_fatal("Found {checkSta} docs without station")
    return(FALSE)
  }
  logger::log_info("Consistency checks complete.")
  return(TRUE)
}
