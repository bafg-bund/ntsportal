
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
