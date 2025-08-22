
setOldClass(c("elasticsearch.Elasticsearch", "elasticsearch._sync.client._base.BaseClient", "python.builtin.object"))
setOldClass(c("python.builtin.module", "python.builtin.object"))

#' A DbComm interface using the Python back-end
#' @description this object implements the DbComm interface 
#'
#' @slot client A python elasticsearch client object
#' @slot dsl The python elasticsearch.dsl import module
#' rdname PythonDbComm
setClass(
  "PythonDbComm",
  contains = "DbComm",
  slots = c(
    client = "elasticsearch.Elasticsearch",
    dsl = "python.builtin.module"
  )
)

#' Build Python DB communications interface
#'
#' A generator for the `PythonDbComm` class.
#'
#' @param ring The name of the keyring where NTSPortal credentials are stored, see `connectNtsportal()` 
#'
#' @returns PythonDbComm
#' @export
#' @rdname PythonDbComm
PythonDbComm <- function(ring = "ntsportal") {
  unlockRing(ring)
  ntspCred <- getCred(ring)
  client <- pyElasticSearchComm$getDbClient(ntspCred[1], ntspCred[2], getOption("ntsportal.elasticsearchHostUrl"))
  dsl <- reticulate::import("elasticsearch.dsl")
  new("PythonDbComm", client = client, dsl = dsl)
}

# Methods in alphabetical order 

# addValueToArray ####
setMethod("addValueToArray", "PythonDbComm", function(dbComm, tableName, field, value, searchBlock = list()) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  arrayCheck(tableName, field, searchBlock)
  tryCatch({
    ubq <- dbComm@dsl$UpdateByQuery(using = dbComm@client, index = tableName)$
      update_from_dict(searchBlock)$
      script(source=glue("if (!ctx._source.{field}.contains(params.newValue)) ctx._source.{field}.add(params.newValue)"), params = list(newValue = value))
    resp <- ubq$execute()
  },
  error = function(cnd)
    warning("Error in addValueToArray: ", conditionMessage(cnd))
  )
  numberUpdatedMessage(resp)
  refreshTable(dbComm, tableName)
})

# appendRecords ####
setMethod("appendRecords", "PythonDbComm", function(dbComm, tableName, records) {
  records <- lapply(records, unclass)  # enable conversion to Python dict
  pyIngestModule$appendRecordsToTable(tableName, records, dbComm@client)
  refreshTable(dbComm, tableName)
  message("Records appended to table ", tableName)
})
# deleteRow ####
setMethod("deleteRow", "PythonDbComm", function(dbComm, tableName, searchBlock) {
  resp <- dbComm@client$delete_by_query(index = tableName, body = searchBlock)
  refreshTable(dbComm, tableName)
  message(resp$body$deleted, " row(s) deleted")
})

# deleteTable ####
#' @rdname deleteTable
#' @aliases deleteTable,PythonDbComm-method
setMethod("deleteTable", "PythonDbComm", function(dbComm, tableName) {
  if (isTable(dbComm, tableName)) {
    tryCatch(
      resp <- dbComm@dsl$Index(tableName)$delete(using = dbComm@client),
      error = function(cnd) {
        warning("deleteTable Error. Message: ", conditionMessage(cnd))
      }
    )
    if (resp$body$acknowledged)
      message("Table deleted")
  } else {
    message("Table not found")
  }
})

# closeTable ####
closeTable <- function(dbComm, tableName) {
  resp <- dbComm@client$indices$close(index = tableName)
  invisible(resp$body$acknowledged)
} 

# copyTable ####
setMethod("copyTable" , "PythonDbComm", function(dbComm, tableName, newTableName, mappingType) {
  createNewTable(dbComm, newTableName, mappingType)
  dbComm@client$reindex(source = list(index = tableName), dest = list(index = newTableName))
  refreshTable(dbComm, newTableName)
})

# createNewTable ####
setMethod("createNewTable" , "PythonDbComm", function(dbComm, tableName, mappingType) {
  mapping <- getMapping(mappingType)
  resp <- dbComm@client$indices$create(index = tableName, body = mapping)
  if (resp$body$acknowledged)
    message("Table created")
})
# getAliasTable ####
setMethod("getAliasTable" , "PythonDbComm",  function(dbComm, aliasName) {
  stopifnot(length(aliasName) == 1)
  resp <- dbComm@client$cat$aliases(name=aliasName)
  if (resp$body == "")
    return("")
  respTab <- read.delim(text = resp$body, header = F, sep = " ") 
  respTab[, 2]
})


# getNrow ####
#' @rdname getNrow
#' @aliases getNrow,PythonDbComm-method
setMethod("getNrow", "PythonDbComm",function(dbComm, tableName, searchBlock = list()) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(searchBlock)
  s$count()
})


# getTableAsRecords ####
setMethod(
  "getTableAsRecords", 
  "PythonDbComm", 
  function(dbComm, tableName, searchBlock = list(), fields = "*", recordConstructor = newNtspRecord) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(searchBlock)$source(fields = fields)
  message(s$count(), " records will be retrieved")
  if (s$count() > 1e6)
    stop("Exceeded the maximum docs that may be retrieved (1e6), refine the query using the searchBlock argument (query DSL)")
  reticulate::iterate(s$iterate(), function(hit) {
    recordConstructor(hit$to_dict())
  })
})

# getTableAsTibble ####
setMethod("getTableAsTibble", "PythonDbComm", function(dbComm, tableName, searchBlock = list(), fields = "*") {
  recs <- getTableAsRecords(dbComm, tableName, searchBlock, fields = fields)
  convertRecordsToTibble(recs)
})

convertRecordsToTibble <- function(recs) {
  unitaryFields <- getUnitaryFields(recs)
  tibbleRowsList <- map(recs, \(rec) getTibbleRow(rec, unitaryFields))
  dplyr::bind_rows(tibbleRowsList)
} 
getUnitaryFields <- function(recs) {
  fields <- reduce(map(recs, names), union)
  fieldLengths <- map(recs, \(rec) map_int(fields, \(f) length(rec[[f]])))
  fields[pmap_int(fieldLengths, max) == 1]
}
getTibbleRow <- function(rec, unitaryFields) {
  unclass(rec) |> 
    tibble::enframe() |> 
    tidyr::pivot_wider() |> 
    tidyr::unnest(cols = any_of(unitaryFields))
}


# getUniqueValues ####
setMethod("getUniqueValues", "PythonDbComm", function(dbComm, tableName, field, maxLength = 10000) {
  resp <- dbComm@client$search(
    index = tableName, 
    body = list(aggs = list(uniques = list(terms = list(field = field, size = maxLength))))
  )
  map_chr(resp$body$aggregations$uniques$buckets, function(x) x$key)
})

# isTable ####
setMethod("isTable", "PythonDbComm", function(dbComm, tableName) {
  dbComm@dsl$Index(tableName)$exists(using = dbComm@client)
})

# ping ####
#' @rdname ping
#' @aliases ping,PythonDbComm-method
setMethod("ping", "PythonDbComm", function(dbComm) {
  dbComm@client$ping()
})

# refreshTable ####
setMethod("refreshTable", "PythonDbComm", function(dbComm, tableName) {
  invisible(dbComm@dsl$Index(tableName)$refresh(using=dbComm@client))
})

# replaceValueInField ####
setMethod("replaceValueInField", "PythonDbComm", function(dbComm, tableName, field, oldValue, newValue) {
  setValueInField(dbComm, tableName, field, value = newValue, 
    searchBlock = list(query = list(term = rlang::list2(!!field := oldValue))))
})

# setValueInField ####
setMethod("setValueInField", "PythonDbComm", function(dbComm, tableName, field, value, searchBlock = list()) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  tryCatch({
    ubq <- dbComm@dsl$UpdateByQuery(using = dbComm@client, index = tableName)$
      update_from_dict(searchBlock)$
      script(source=glue("ctx._source.{field} = params.newValue"), params = list(newValue = value))
    resp <- ubq$execute()
  },
  error = function(cnd)
    warning("Error in setValueInField: ", conditionMessage(cnd))
  )
  numberUpdatedMessage(resp)
  refreshTable(dbComm, tableName)
})

# removeValueFromArray ####
setMethod("removeValueFromArray", "PythonDbComm", function(dbComm, tableName, field, value, searchBlock = list()) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  arrayCheck(tableName, field, searchBlock)
  tryCatch({
    ubq <- dbComm@dsl$UpdateByQuery(using = dbComm@client, index = tableName)$
      update_from_dict(searchBlock)$
      script(source=glue("ctx._source.{field}.removeIf(item -> item == params.valueToRemove)"), params = list(valueToRemove = value))
    resp <- ubq$execute()
  },
  error = function(cnd)
    warning("Error in removeValueFromArray: ", conditionMessage(cnd))
  )
  numberUpdatedMessage(resp)
  refreshTable(dbComm, tableName)
})

arrayCheck <- function(tableName, field, searchBlock) {
  if (!isArray(tableName, field, searchBlock)) 
    stop("The field ", field, " is not an array in every doc. First convert your field to an array.")
}

isArray <- function(tableName, field, searchBlock) {
  dbComm <- getDbComm()
  recs <- getTableAsRecords(dbComm, tableName, searchBlock, fields = field)
  all(sapply(recs, \(rec) length(rec[[field]]) > 1))
}

numberUpdatedMessage <- function(resp) {
  if (resp$success())
    message("Change accepted, ", resp$updated, " documents updated")
}

# show ####
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

viewTable <- function(dbComm, tableName) {
  View(getTableAsTibble(dbComm, tableName))
}

matchAllIfEmpty <- function(searchBlock) {
  if (length(searchBlock) == 0) {
    return(list(query = list(match_all = stats::setNames(list(), character(0)))))
  } else {
    return(searchBlock)
  }
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
