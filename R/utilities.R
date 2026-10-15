

glue_json <- function(stringToModify) {
  x <- glue::glue(stringToModify, .envir = parent.frame(1), .open = "[[", .close = "]]")
  gsub("[\r\n]", "", x)
}

tconvert <- function(unixtime) {
  as.POSIXct(unixtime, origin = "1970-01-01 00:00")
}

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

getMappingProperties <- function(mappingType) {
  getMapping(mappingType)$mappings$properties
}

getMapping <- function(mappingType) {
  stopifnot(mappingType %in% getNtsportalTableTypes())
  pth <- fs::path_package("ntsportal", "mappings", glue("{mappingType}_index_mappings.json"))
  jsonlite::read_json(pth)
}

getNtsportalTableTypes <- function() {
  c("feature", "msrawfiles", "analysis_dbas", "spectral_library", "nondetect_dbas")
}

testConnection <- function() {
  dbComm <- getDbComm()
  if (!ping(dbComm))
    stop("Unable to connect to elasticSearch")
}

#' Get a field from a list of NTSP-records
#' @param listRecords list of `ntspRecord`
#' @param fieldName name of field of interest
#' @param quiet boolean, silence messages when nothing found
#' @returns for unitary fields, a vector of appropriate type, otherwise a list
#' @export
getField <- function(listRecords, fieldName, quiet = FALSE) {
  if (!fieldAvailable(listRecords, fieldName)) {
    if (!quiet)
      message("Field ", fieldName, " not found in any docs")
    return(rep(NA, length(listRecords)))
  } 
  values <- lapply(listRecords, getValueOrEmpty, field = fieldName)
  sizes <- vapply(values, length, numeric(1))
  if (all(sizes == 1)) {
    unname(unlist(values))
  } else {
    unname(values)
  }
}

splitRecordsByDir <- function(recsToProcess) {
  paths <- getField(recsToProcess, "path")
  if (length(paths) > 0) {
    dirs <- dirname(getField(recsToProcess, "path"))
  } else {
    dirs <- character(0)
  }
  split(recsToProcess, dirs)
}

#' Turn mappings into a tibble for documentation
#'
#' @param mappingType name of mapping type, e.g. "feature"
#'
#' @returns tbl_df of mappings with metadata
#' @export
makeMappingsTbl <- function(mappingType) {
  m <- getMapping(mappingType)
  properties <- m$mappings$properties
  mainLevel <- makeTblFromPropertiesList(properties)
  
  nestedFields <- mainLevel[mainLevel$type == "nested", "field", drop = T]
  nestedLevel <- mainLevel[0, ]
  if (length(nestedFields) > 0)
    nestedLevel <- list_rbind(map(nestedFields, \(f) makeTblNestedField(f, properties)))
  
  runTimeLevel <- mainLevel[0, ]
  runtime <- m$mappings$runtime
  if (!is.null(runtime)) 
    runTimeLevel <- makeTblRuntimeFields(runtime)
  
  rbind(mainLevel, nestedLevel, runTimeLevel) |> arrange(field)
}

makeTblNestedField <- function(parentField, properties) {
  propertiesNested <- properties[[parentField]]$properties
  newTbl <- makeTblFromPropertiesList(propertiesNested)
  newTbl$field <- paste0(parentField, ".", newTbl$field)
  newTbl
}

makeTblRuntimeFields <- function(runtimeProperties) {
  newTbl <- makeTblFromPropertiesList(runtimeProperties)
  newTbl$type <- paste(newTbl$type, "(runtime)")
  newTbl
}

makeTblFromPropertiesList <- function(properties) {
  tibble(
    field = names(properties),
    type = map_chr(properties, \(p) p$type),
    description = map_chr(properties, \(p) ifelse(is.null(p$meta$description), "", p$meta$description)),
    unit = map_chr(properties, \(p) ifelse(is.null(p$meta$unit), "", p$meta$unit)),
    example = map_chr(properties, \(p) ifelse(is.null(p$meta$example), "", p$meta$example))
  )
}


#' Install shell interface to path
#'
#' @param destDir path into which the script will be linked. Must be already in system PATH
#'
#' @export
installShellInterface <- function(destDir = "~/bin") {
  scriptPath <- fs::path_package("ntsportal", "shellInterface", "ntsp")
  destDir <- normalizePath(destDir)
  
  # Check if destDir exists and is in system PATH
  if (!dir.exists(destDir)) 
    stop("destDir does not exist, create directory and add to system path")
  pathEnv <- strsplit(Sys.getenv("PATH"), .Platform$path.sep)[[1]]
  if (!is.element(destDir,normalizePath(pathEnv))) {
    stop(destDir, " is not currently in your system PATH environment variable.")
  }
  
  target <- file.path(destDir, "ntsp")
  
  if (file.exists(target)) 
    file.remove(target)
  res <- file.symlink(scriptPath, target)
  if (res) {
    message("Successfully symlinked 'ntsp' to ", target)
  } else {
    message("Failed symlinked 'ntsp' to ", target)
  }
}

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
