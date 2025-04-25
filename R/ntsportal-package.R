


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
#' @import glue
#' @import reticulate
#' @import purrr
#' @import methods
NULL

.onLoad <- function(libname, pkgname) {
  pathToPyModules <- fs::path_package("ntsportal", "pythonElasticComm")
  options(warn = 1)
  options(ntsportal.dbComm = PythonDbComm)
  elasticSearchComm <<- reticulate::import_from_path(module = "elasticSearchComm", path = pathToPyModules, delay_load = T)
  ingestModule <<- reticulate::import_from_path(module = "ingest_main", path = pathToPyModules, delay_load = T)
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nBefore starting, use connectNtsportal() to save your login credentials\n")
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal