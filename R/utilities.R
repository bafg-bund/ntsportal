


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
#' is done within 1% accuracy since the cardinality computation is fuzzy
#' (HyperLogLog++ algorithm).
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
      "script": "emit(doc[\'intensity\'].value +
      \' \' + doc[\'station\'].value + \' \' + doc[\'mz\'].value + \' \'
      + doc[\'rt\'].value + \' \' + doc[\'pol\'].value + \' \' + doc[\'start\'].value)"
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
  # cardinality calculation is fuzzy, assume at least 1% accuracy
  return(abs(totDocs - numDiff) / totDocs < 0.01)
}
