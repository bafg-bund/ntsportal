#' DbComm interface
setClass("DbComm", contains = "VIRTUAL")

#' @export
getDbComm <- function() {
  getOption("ntsportal.dbComm")()
}

setGeneric("refreshTable", function(dbComm, tableName) standardGeneric("refreshTable"))
setGeneric("deleteTable", function(dbComm, tableName) standardGeneric("deleteTable"))
setGeneric("isTable", function(dbComm, tableName) standardGeneric("isTable"))
setGeneric("createNewTable", function(dbComm, tableName, mappingType) standardGeneric("createNewTable"))
setGeneric("copyTable", function(dbComm, tableName, newTableName, mappingType) standardGeneric("copyTable"))
setGeneric("getUniqueValues", function(dbComm, tableName, field, maxLength = 10000) standardGeneric("getUniqueValues"))
setGeneric("replaceValueInField", function(dbComm, tableName, field, oldValue, newValue) standardGeneric("replaceValueInField"))
setGeneric("getNrow", function(dbComm, tableName, queryBlock = list()) standardGeneric("getNrow"))
setGeneric("deleteRow", function(dbComm, tableName, queryBlock = list()) standardGeneric("deleteRow"))
setGeneric("getAliasTable", function(dbComm, aliasName) standardGeneric("getAliasTable"))
setGeneric("appendRecords", function(dbComm, tableName, records) standardGeneric("appendRecords"))

#' Test the DB-Connection
#' 
#' @param queryBlock list coercible to json for elasticSearch REST API (Query DSL)
#' @param recordConstructor function to construct new records
#'  
#' @return TRUE when connection active
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric(
  "getTableAsRecords", 
  function(dbComm, tableName, queryBlock = list(), recordConstructor = newNtspRecord) 
    standardGeneric("getTableAsRecords")
)


#' Test the DB-Connection
#' 
#' @return TRUE when connection active
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric("ping", function(dbComm) standardGeneric("ping")) 
