

#' Get ID based on search parameters
#'
#' @param rfIndex index name for rawfiles index 
#' @param isBlank boolean default is FALSE
#' @param pol Either "pos" or "neg", default "pos"
#' @param station Station ID name 
#' @param matrix character default is "spm"
#' @param duration numeric, default 365
#' @param instrumentName instrument used for measurement
#'
#' @return ES-ID of template record in msrawfiles index, returns empty string when no id found
#' @export
#'
findTemplateId <- function(rfIndex, blank = FALSE, pol = "pos", station = "rhein_ko_l", matrix = "spm", 
                           duration = "P1D", instrumentName = "wasser_tof") {
  idTable <- getTableByEsql(glue('
   FROM {rfIndex} METADATA _id | 
   WHERE blank == {blank} AND pol == "{pol}" AND station == "{station}" AND matrix == "{matrix}" AND 
     duration == "{duration}" AND instrument_name == "{instrumentName}" | 
   KEEP _id |
   LIMIT 1
  '))
  if (nrow(idTable) == 0) {
    warning("Search returned no hits")
    return("")
  }
  ids <- pull(idTable, "_id")
  
  ids[1]
}

#' Get a template ID based on batchname pattern
#'
#' @param rfIndex index name for rawfiles index 
#' @param batchName batchname pattern (wildcards allowed, uses ES|QL `LIKE` command)
#' @param isBlank boolean default is FALSE
#'
#' @return ES-ID of template record in msrawfiles index
#' @export
#'
findTemplateByBatchname <- function(rfIndex, batchName, blank = FALSE) {
  idTable <- getTableByEsql(glue('
   FROM {rfIndex} METADATA _id | 
   WHERE blank == {blank} AND batchname LIKE """{batchName}""" | 
   KEEP _id |
   LIMIT 1
  '))
  if (nrow(idTable) == 0) {
    warning("Search returned no hits")
    return("")
  }
  ids <- pull(idTable, "_id")
  ids[1]
}

#' Get a template ID based on ES|QL search
#'
#' @param rfIndex index name for rawfiles index 
#' @param esqlWhere ES|QL WHERE fragment as text
#'
#' @return ES-ID of template record in msrawfiles index, empty string for no hits
#' @export
#'
getIdsByEsql <- function(rfIndex, where) {
  idTable <- getTableByEsql(glue('
   FROM {rfIndex} METADATA _id | 
   WHERE {where} | 
   KEEP _id |
   LIMIT 10000
  '))
  if (nrow(idTable) == 0) {
    warning("Search returned no hits")
    return("")
  }
  pull(idTable, "_id")
}


# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal