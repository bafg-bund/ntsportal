


#' @keywords internal 
"_PACKAGE"

#' ntsportal: A package for non-target data management
#'
#'
#' @name ntsportal
#' @import dplyr
#' @import logger
#' @import future
#' @import ntsworkflow
#' @import rlang
#' @import glue
#' @import reticulate
NULL

.onLoad <- function(libname, pkgname) {
  pathToPyModules <- fs::path_package("ntsportal", "pythonElasticComm")
  elasticSearchComm <<- reticulate::import_from_path(module = "elasticSearchComm", path = pathToPyModules, delay_load = T)
  ingestModule <<- reticulate::import_from_path(module = "ingest_main", path = pathToPyModules, delay_load = T)
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nBefore starting, use connectNtsportal() to create connection object to ElasticSearch\n")
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal