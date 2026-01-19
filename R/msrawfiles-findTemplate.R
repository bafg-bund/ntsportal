

#' Get ID based on search parameters
#'
#' @param rfIndex index name for rawfiles index 
#' @param isBlank boolean default is FALSE
#' @param pol Either "pos" or "neg", default "pos"
#' @param station Station ID name 
#' @param matrix character default is "spm"
#' @param duration numeric, default 365
#'
#' @return ES-ID of template record in msrawfiles index
#' @export
#'
findTemplateId <- function(rfIndex, blank = FALSE, pol = "pos", station = "rhein_ko_l", matrix = "spm", duration = "P1D") {
  idTable <- getTableByEsql(glue('FROM {rfIndex} [METADATA _id]| WHERE blank == {blank} and pol == "{pol}" and 
                                  station == "{station}" and matrix == "{matrix}" and duration == "{duration}" | KEEP _id'))
  ids <- pull(idTable, "_id")
  if (length(ids) == 0) {
    warning("Search returned no hits")
    return("")
  }
  ids[1]
}

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal