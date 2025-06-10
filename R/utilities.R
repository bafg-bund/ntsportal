# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

glue_json <- function(stringToModify) {
  x <- glue::glue(stringToModify, .envir = parent.frame(1), .open = "[[", .close = "]]")
  gsub("[\r\n]", "", x)
}

tconvert <- function(unixtime) {
  as.POSIXct(unixtime, origin = "1970-01-01 00:00")
}


#' Handle errors from ElasticSearch
#' 
#' Errors from ElasticSearch are returned by package 'elastic' as text. This
#' function will take the error condition and take appropriate action.
#'
#' @param thisCnd Error condition thrown in the `tryCatch` context
#'
#' @return No return value
es_error_handler <- function(thisCnd) {
  if (is.character(conditionMessage(thisCnd)) && 
      grepl("429", conditionMessage(thisCnd))) {
    logger::log_warn("Error 429 from ElasticSearch, taking a 1 h break")
    Sys.sleep(3600)
  }
  if (is.character(conditionMessage(thisCnd)) && 
      grepl("404", conditionMessage(thisCnd))) {
    logger::log_warn("Error 404 from ElasticSearch, possibly because ID was
                     not found")
  }
}

connectSqlite <- function(pth) {
  DBI::dbConnect(RSQLite::SQLite(), pth)
}


free_gb <- function() {
  x <- system("free -g", intern = T)
  x <- strsplit(x[2], "\\s+")
  as.numeric(x[[1]][length(x[[1]])])
}  




#' Get search results for more than 10000 docs by pagination. 
#' 
#' local function
#'
#' @param indexName Name of ElasticSearch index
#' @param searchBody search body, default will return all docs
#' @param sort Sort argument passed onto elastic::Search. Defines which field 
#' the results are sorted by (best if this is unique for all docs to avoid ties) 
#' currently can only be one field and may not be '_id'.
#' @param totalSize 
#' @param ... further arguments to elastic::Search, asdf argument does not work for now.
#' @export
#' @return ElasticSearch API response as a list
#'
esSearchPaged <- function(
    indexName, 
    searchBody = list(query = list(match_all = stats::setNames(list(), character(0)))), 
    sort, 
    totalSize = Inf, 
    ...
  ) {
    
  stopifnot(length(sort) == 1)
  newSize <- 10000
  if (totalSize < 10000)
    newSize <- totalSize
  createEscon()
  res1 <- elastic::Search(escon, indexName, body = searchBody, sort = sort, 
                          size = newSize, ...)
  numHits <- length(res1$hits$hits)
  
  if (res1$hits$total$value < 10000 || numHits < 10000)
    return(res1)
  
  # Get the rest of the results
  searchAfter <- res1$hits$hits[[numHits]]$sort[[1]]
  
  if (!is.list(searchBody) && is.character(searchBody)) {
    stop("Do not use a string-based search, convert to a list-based search")
  } else if (is.list(searchBody)) {
    searchBodyList <- searchBody
  } else {
    stop("Unknown searchBody format") 
  }
  
  searchBodyList$search_after <- list(searchAfter)
  
  repeat {
    nextRes <- elastic::Search(escon, indexName, body = searchBodyList, sort = sort, 
                               size = 10000, ...)
    newNumHits <- length(nextRes$hits$hits)
    if (newNumHits > 0)
      res1$hits$hits <- append(res1$hits$hits, nextRes$hits$hits)
    
    if (newNumHits < 10000) {
      break
    } else {
      newSearchAfter <- nextRes$hits$hits[[newNumHits]]$sort[[1]]
      searchBodyList$search_after <- list(newSearchAfter)
    }
      
  }
  res1  
}

es_remove_by_filename <- function(index, filenames) {
  # Must be a dbas index, not msrawfiles, otherwise this function would be bad!
  stopifnot(grepl("ntsp_index_dbas", index))
  stopifnot(is.character(filenames), length(filenames) > 0)
  qbod <- list(
    query = list(
      terms = list(
        filename = as.list(filenames)
      )
    )
  )
  suc <- TRUE
  if (elastic::Search(escon, index, body = qbod, size = 0)$hits$total$value == 0) {
    logger::log_info("No docs found in index {index}")
    return(invisible(FALSE))
  }
  
  
  tryCatch(
    res <- elastic::docs_delete_by_query(escon, index, body = qbod, refresh = "true"),
    error = function(cnd) {
      logger::log_error("Could not remove docs from {index}, \
                        {conditionMessage(cnd)}")
      suc <<- FALSE
    }
  )
  if (suc) {
    invisible(TRUE)  
  } else {
    invisible(FALSE)
  }
}

# Function to print a search query string to be used for log files
# Takes a vector of esids and a vector of fields for _source
build_es_query_for_ids <- function(ids, toShow) {
  message("\nUse the following query to search for the docs:")
  cat(
    sprintf('GET %s/_search
  {
    "query": {
      "ids": {
        "values": [%s]
      }
    },
    "_source": [%s]
  }\n', 
            rfindex, 
            paste(shQuote(ids, type = "cmd") , collapse = ", "),
            paste(shQuote(toShow, type = "cmd") , collapse = ", ")
    )
  )
  invisible(T)
}

getMapping <- function(mappingType) {
  stopifnot(mappingType %in% c("dbas", "msrawfiles", "nts", "analysis_dbas", "spectral_library"))
  pth <- fs::path_package("ntsportal", "mappings", glue("{mappingType}_index_mappings.json"))
  jsonlite::read_json(pth)
}

testConnection <- function() {
  dbComm <- getDbComm()
  if (!ping(dbComm))
    stop("Unable to connect to elasticSearch")
}
