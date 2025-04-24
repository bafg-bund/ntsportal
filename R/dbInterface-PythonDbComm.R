
#' DbComm interface
setClass("DbComm", contains = "VIRTUAL")


setOldClass(c("elasticsearch.Elasticsearch", "elasticsearch._sync.client._base.BaseClient", "python.builtin.object"))
setOldClass(c("python.builtin.module", "python.builtin.object"))

#' A DbComm interface using the python back-end
#' 
#' @slot client A python elasticsearch client object
#' @slot dsl The python elasticsearch.dsl import module
setClass(
  "PythonDbComm",
  contains = "DbComm",
  slots = c(
    client = "elasticsearch.Elasticsearch",
    dsl = "python.builtin.module"
  )
)

#' Create connection to elasticsearch
#'
#' @param ring 
#'
#' @returns PythonDbComm
#' @export
#'
PythonDbComm <- function(ring = "ntsportal") {
  ntspCred <- getCred(ring)
  client <- elasticSearchComm$getDbClient(ntspCred[1], ntspCred[2], "https://elastic.dmz.bafg.de")
  dsl <- import("elasticsearch.dsl")
  new("PythonDbComm", client = client, dsl = dsl)
}


#' @rdname DbComm-methods
#' @aliases show,PythonDbComm-method
setMethod("show", "PythonDbComm", function(object) {
  resp <- object@client$info()
  cat(
    is(object)[[1]], "\n",
    glue("  cluster name: {resp$body$cluster_name}"), "\n",
    glue("  status: {resp$meta$status}"), "\n",
    sep = ""
  )
})

#' Test the DB-Connection
#' 
#' @return TRUE when connection active
#' @export
#' @docType methods
#' @rdname DbComm-methods
setGeneric("ping", function(dbComm) standardGeneric("ping"))

#' @rdname DbComm-methods
#' @aliases ping,PythonDbComm-method
setMethod("ping", "PythonDbComm", function(dbComm) {
  resp <- dbComm@client$ping()
})

setGeneric("refreshTable", function(dbComm, tableName) standardGeneric("refreshTable"))
setMethod("refreshTable", "PythonDbComm", function(dbComm, tableName) {
  invisible(dbComm@dsl$Index(tableName)$refresh(using=dbComm@client))
})

setGeneric("deleteTable", function(dbComm, tableName) standardGeneric("deleteTable"))
setMethod("deleteTable", "PythonDbComm", function(dbComm, tableName) {
  tryCatch(
    resp <- dbComm@dsl$Index(tableName)$delete(using = dbComm@client),
    error = function(cnd) {
      warning("deleteTable Error. Message: ", conditionMessage(cnd))
    }
  )
  if (resp$body$acknowledged)
    message("Table deleted")
})

setGeneric("isTable", function(dbComm, tableName) standardGeneric("isTable"))
setMethod("isTable", "PythonDbComm", function(dbComm, tableName) {
  dbComm@dsl$Index(tableName)$exists(using = dbComm@client)
})

setGeneric("createNewTable", function(dbComm, tableName, mappingType) standardGeneric("createNewTable"))
setMethod("createNewTable" , "PythonDbComm", function(dbComm, tableName, mappingType) {
  mapping <- getMapping(mappingType)
  dbComm@client$indices$create(index = tableName, body = mapping)
})

setGeneric("copyTable", function(dbComm, tableName, newTableName, mappingType) standardGeneric("copyTable"))
setMethod("copyTable" , "PythonDbComm", function(dbComm, tableName, newTableName, mappingType) {
  createNewTable(dbComm, newTableName, mappingType)
  dbComm@client$reindex(source = list(index = tableName), dest = list(index = newTableName))
  refreshTable(dbComm, newTableName)
})