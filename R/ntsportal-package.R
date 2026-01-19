


#' @keywords internal 
"_PACKAGE"

#' ntsportal: A package for non-target data management
#'
#' @name ntsportal
#' @import dplyr
#' @import logger
#' @import future
#' @importFrom glue glue
#' @importFrom purrr map
#' @importFrom purrr map2
#' @importFrom purrr map_lgl
#' @importFrom purrr map_chr
#' @importFrom purrr map_int
#' @importFrom purrr map_dbl
#' @importFrom purrr pmap_int
#' @importFrom purrr map_at
#' @importFrom purrr walk
#' @importFrom purrr walk2
#' @importFrom purrr pluck
#' @importFrom purrr reduce
#' @importFrom purrr keep
#' @importFrom purrr list_rbind
#' @importFrom purrr list_c
#' @importFrom tibble as_tibble
#' @importFrom tibble as_tibble_row
#' @importFrom tibble tibble
#' @import methods
#' @import R6
#' @import cli
NULL

.onLoad <- function(libname, pkgname) {
  pathToPyModules <- fs::path_package("ntsportal", "pythonElasticComm")
  options(warn = 1)
  options(ntsportal.dbComm = PythonDbComm)
  #options(ntsportal.elasticsearchHostUrl = "https://ntsportal.bafg.de/es-api")
  options(ntsportal.elasticsearchHostUrl = "https://elastic.dmz.bafg.de")
  options(cli.progress_clear = FALSE)
  options(cli.progress_show_after = 0)
  if (Sys.info()["sysname"] == "Linux")
    options(keyring_backend = "file")
  if ("elasticsearch" %in% reticulate::py_list_packages()$package) {
    pyElasticSearchComm <<- reticulate::import_from_path(module = "elasticSearchComm", path = pathToPyModules, delay_load = T)
    pyIngestModule <<- reticulate::import_from_path(module = "ingest", path = pathToPyModules, delay_load = T)
  } else {
    warning("Could not find Python dependencies, check README.")
  }
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nBefore starting, use connectNtsportal() to save your login credentials\n")
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
