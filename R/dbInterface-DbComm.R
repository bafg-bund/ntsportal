#' DbComm interface
setClass("DbComm", contains = "VIRTUAL")

#' Make a DbComm interface object
#' @description Uses the constructor given by the `ntsportal.dbComm` option
#' @export
getDbComm <- function() {
  getOption("ntsportal.dbComm")()
}

# Generics in alphabetical order ####

#' Add value to an array field
#' @param dbComm DbComm connection object
#' @param tableName Name of table in database, wildcards are permitted
#' @param field table column (field) name see mappings
#' @param value value to set or add
#' @param searchBlock list coercible to json for Elasticsearch Search API (Query DSL)
#' @seealso \link{`removeValueFromArray`}
#' @export
setGeneric("addValueToArray", function(dbComm, tableName, field, value, searchBlock = list()) standardGeneric("addValueToArray"))
setGeneric("appendRecords", function(dbComm, tableName, records) standardGeneric("appendRecords"))
setGeneric("deleteRow", function(dbComm, tableName, searchBlock = list()) standardGeneric("deleteRow"))

#' Delete a table
#' @description The named table is removed from the database
#' @inheritParams addValueToArray
#' @export
setGeneric("deleteTable", function(dbComm, tableName) standardGeneric("deleteTable"))

#' Does a table exist?
#' @inheritParams deleteTable
#' @returns logical, true if table exists
#' @export
setGeneric("isTable", function(dbComm, tableName) standardGeneric("isTable"))
setGeneric("copyTable", function(dbComm, tableName, newTableName, mappingType) standardGeneric("copyTable"))
setGeneric("createNewTable", function(dbComm, tableName, mappingType) standardGeneric("createNewTable"))
setGeneric("getAliasTable", function(dbComm, aliasName) standardGeneric("getAliasTable"))

#' Get the number of rows in a table
#' @description The number of rows (or doc count) is returned for a given table (wildcards allowed). 
#' The `searchBlock` argument can be used to limit the results to a search query.
#' @inheritParams addValueToArray
#' @returns integer
#' @export
setGeneric("getNrow", function(dbComm, tableName, searchBlock = list()) standardGeneric("getNrow"))

#' Get a table as a list of ntspRecords 
#' @description The table is retrieved as `list` of `ntspRecord`s or subclass thereof. The `recordConstructor` can be used to
#' make a subclass, e.g., to make `msrawfilesRecord`s use `newMsrawfilesRecord`. The default makes the generic `ntspRecord`.
#' @inheritParams getNrow
#' @param fields Fields to include in the response, wildcards are permitted, default is all fields.
#' @param recordConstructor function to construct new records
#' @return A `list` of `ntspRecord` objects
#' @export
setGeneric(
  "getTableAsRecords", 
  function(dbComm, tableName, searchBlock = list(), fields = "*", recordConstructor = newNtspRecord) 
    standardGeneric("getTableAsRecords")
)

#' Get a table as a tibble
#' @description Works similarly to `getTableAsRecords()` but reformats the `ntspRecords` into a `tibble`. 
#' @inheritParams getTableAsRecords
#' @return A tibble
#' @seealso \link{`getTableAsRecords`}
#' @export
setGeneric(
  "getTableAsTibble",
  function(dbComm, tableName, searchBlock = list(), fields = "*") standardGeneric("getTableAsTibble")
)

setGeneric("getUniqueValues", function(dbComm, tableName, field, maxLength = 10000) standardGeneric("getUniqueValues"))
setGeneric("refreshTable", function(dbComm, tableName) standardGeneric("refreshTable"))

#' Remove a value from an array field 
#' @inheritParams addValueToArray
#' @seealso \link{`addValueToArray`}
#' @export
setGeneric("removeValueFromArray", function(dbComm, tableName, field, value, searchBlock = list()) standardGeneric("removeValueFromArray"))

setGeneric("replaceValueInField", function(dbComm, tableName, field, oldValue, newValue) standardGeneric("replaceValueInField"))

#' Set the value of a field in a table
#' @description Used to change the value of a field for all rows (i.e. docs) returned by `seearchBlock`
#' @inheritParams addValueToArray
#' @export
setGeneric("setValueInField", function(dbComm, tableName, field, value, searchBlock = list()) standardGeneric("setValueInField"))

#' Test the DB-Connection
#' @inheritParams isTable
#' @return TRUE when connection active
#' @export
#' @docType methods
setGeneric("ping", function(dbComm) standardGeneric("ping")) 

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
