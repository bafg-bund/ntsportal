


#' @keywords internal 
"_PACKAGE"

#' ntsportal: A package for non-target data management
#'
#'
#' @name ntsportal
#' @import dplyr
#' @import logger
#' @import future
#' @import glue
#' @import reticulate
#' @import purrr
#' @import methods
#' @import R6
#' @import cli
NULL

.onLoad <- function(libname, pkgname) {
  pathToPyModules <- fs::path_package("ntsportal", "pythonElasticComm")
  options(warn = 1)
  options(ntsportal.dbComm = PythonDbComm)
  options(ntsportal.elasticsearchHostUrl = "https://elastic.dmz.bafg.de")
  options(cli.progress_clear = FALSE)
  options(cli.progress_show_after = 0)
  pyElasticSearchComm <<- reticulate::import_from_path(module = "elasticSearchComm", path = pathToPyModules, delay_load = T)
  pyIngestModule <<- reticulate::import_from_path(module = "ingest", path = pathToPyModules, delay_load = T)
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nBefore starting, use connectNtsportal() to save your login credentials\n")
}


# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal