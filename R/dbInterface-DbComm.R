#' DbComm interface
setClass("DbComm", contains = "VIRTUAL")

#' @export
getDbComm <- function() {
  getOption("ntsportal.dbComm")()
}

# Generics in alphabetical order ####

setGeneric("appendRecords", function(dbComm, tableName, records) standardGeneric("appendRecords"))
setGeneric("deleteRow", function(dbComm, tableName, searchBlock = list()) standardGeneric("deleteRow"))
setGeneric("deleteTable", function(dbComm, tableName) standardGeneric("deleteTable"))

#' Does a table exist?
#'
#' @param dbComm DbComm connection object
#' @param tableName Name of table in database
#'
#' @returns logical, TRUE if table exists
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric("isTable", function(dbComm, tableName) standardGeneric("isTable"))
setGeneric("copyTable", function(dbComm, tableName, newTableName, mappingType) standardGeneric("copyTable"))
setGeneric("createNewTable", function(dbComm, tableName, mappingType) standardGeneric("createNewTable"))
setGeneric("getAliasTable", function(dbComm, aliasName) standardGeneric("getAliasTable"))
setGeneric("getNrow", function(dbComm, tableName, searchBlock = list()) standardGeneric("getNrow"))

#' Get a table as a list of ntspRecords 
#' 
#' @inheritParams isTable
#' @param searchBlock list coercible to json for elasticSearch REST API (Query DSL)
#' @param recordConstructor function to construct new records
#'  
#' @return A list of ntspRecord objects
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric(
  "getTableAsRecords", 
  function(dbComm, tableName, searchBlock = list(), recordConstructor = newNtspRecord) 
    standardGeneric("getTableAsRecords")
)

#' Get a table as a tibble
#' @inheritParams getTableAsRecords
#' @return A tibble
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric(
  "getTableAsTibble",
  function(dbComm, tableName, searchBlock = list()) standardGeneric("getTableAsTibble")
)
setGeneric("getUniqueValues", function(dbComm, tableName, field, maxLength = 10000) standardGeneric("getUniqueValues"))
setGeneric("refreshTable", function(dbComm, tableName) standardGeneric("refreshTable"))

setGeneric("replaceValueInField", function(dbComm, tableName, field, oldValue, newValue) standardGeneric("replaceValueInField"))

#' Set the value of a field in a table
#' 
#' @inheritParams getTableAsRecords
#' @param field table column to change
#' @param value value to set in field
#'  
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric("setValueInField", function(dbComm, tableName, field, value, searchBlock = list()) standardGeneric("setValueInField"))

#' Test the DB-Connection
#' @inheritParams isTable
#' @return TRUE when connection active
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric("ping", function(dbComm) standardGeneric("ping")) 
