# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal


#' Extract field from docs list
#' 
#' @description
#' a wrapper around vapply or lapply to get values from docs_source object
#' 
#' @param docsSrc docs_source object: list returned by elasticSearch API (only the _source part)
#' @param fieldName field to extract (length 1 character)
#' @param value FUN.VALUE argument for vapply or "unknown", in which case a list is returned
#' @param justone logical return only length one and warn if not all the same
#'
#' @return vector or list of the extracted value
#'
gf <- function(docsSrc, fieldName, value, justone = F) {
  # Check that field is present in all docs
  if (!all(vapply(docsSrc, function(x) fieldName %in% names(x), logical(1))))
    stop("Field ", fieldName, " not found in all docs")
  if (value == "unknown") {
    x <- lapply(docsSrc, "[[", i = fieldName)
  } else {
    x <- vapply(docsSrc, "[[", i = fieldName, FUN.VALUE = value)
  }
  
  if (justone && length(unique(x)) > 1) {
    warning("Extracting the field ", fieldName, " gave more than one unique value, choosing first")
  }
  if (justone) 
    x[1] else x
}

glue_json <- function(...) {
  x <- glue::glue(..., .open = "[[", .close = "]]")
  gsub("[\r\n]", "", x)
}

tconvert <- function(unixtime) {
  as.POSIXct(unixtime, origin = "1970-01-01 00:00")
}

#' Plot an ms2 spectrum for viewing
#'
#' @param ms2Spec data.frame with columns mz and int
#'
#' @return ggplot2 plot object
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

#' Return the number of Gb of free memory on linux server
#'
#' @return numeric length 1
free_gb <- function() {
  x <- system("free -g", intern = T)
  x <- strsplit(x[2], "\\s+")
  as.numeric(x[[1]][length(x[[1]])])
}  


#' Get search results for more than 10000 docs by pagination.
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param index Name of ElasticSearch index
#' @param searchBody search body, if NULL, will return all docs
#' @param sort Sort argument passed onto elastic::Search. Defines which field 
#' the results are sorted by (best if this is unique for all docs to avoid ties) 
#' currently can only be one field and may not be _id.
#' @param totalSize 
#' @param ... further arguments to elastic::Search, asdf argument does not work for now.
#'
#' @return ElasticSearch API response as a list
#'
es_search_paged <- function(escon, index, searchBody = NULL, sort, totalSize = Inf, ...) {
  # initial request
  if (is.null(searchBody)) {
    searchBody <- list(query = list(match_all = list()))
    names(searchBody$query$match_all) <- character()
  }
    
  stopifnot(length(sort) == 1)
  newSize <- 10000
  if (totalSize < 10000)
    newSize <- totalSize
  res1 <- elastic::Search(escon, index, body = searchBody, sort = sort, 
                          size = newSize, ...)
  numHits <- length(res1$hits$hits)
  
  if (res1$hits$total$value < 10000 || numHits < 10000)
    return(res1)
  
  # Get the rest of the results
  searchAfter <- res1$hits$hits[[numHits]]$sort[[1]]
  
  if (!is.list(searchBody) && is.character(searchBody)) {
    stop("Do not use a string-based search, convert to a list-based search")
    searchBodyList <- jsonlite::fromJSON(searchBody)
    
  } else if (is.list(searchBody)) {
    searchBodyList <- searchBody
  } else {
    stop("Unknown searchBody format") 
  }
  
  searchBodyList$search_after <- list(searchAfter)
  
  repeat {
    nextRes <- elastic::Search(escon, index, body = searchBodyList, sort = sort, 
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

