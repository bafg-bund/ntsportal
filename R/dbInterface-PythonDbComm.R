
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
  unlockRing(ring)
  ntspCred <- getCred(ring)
  client <- pyElasticSearchComm$getDbClient(ntspCred[1], ntspCred[2], getOption("ntsportal.elasticsearchHostUrl"))
  dsl <- import("elasticsearch.dsl")
  new("PythonDbComm", client = client, dsl = dsl)
}

# Methods in alphabetical order ####

# appendRecords ####
setMethod("appendRecords", "PythonDbComm", function(dbComm, tableName, records) {
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

# copyTable ####
setMethod("copyTable" , "PythonDbComm", function(dbComm, tableName, newTableName, mappingType) {
  createNewTable(dbComm, newTableName, mappingType)
  dbComm@client$reindex(source = list(index = tableName), dest = list(index = newTableName))
  refreshTable(dbComm, newTableName)
})

# createNewTable ####
setMethod("createNewTable" , "PythonDbComm", function(dbComm, tableName, mappingType) {
  mapping <- getMapping(mappingType)
  dbComm@client$indices$create(index = tableName, body = mapping)
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
setMethod("getNrow", "PythonDbComm",function(dbComm, tableName, searchBlock = list()) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(searchBlock)
  s$count()
})


# getTableAsRecords ####
setMethod("getTableAsRecords", "PythonDbComm", function(dbComm, tableName, searchBlock = list(), recordConstructor = newNtspRecord) {
  searchBlock <- matchAllIfEmpty(searchBlock)
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(searchBlock)
  if (s$count() > 1e6)
    stop("Exceeded the maximum docs that may be retrieved (1e6)")
  iterate(s$iterate(), function(hit) recordConstructor(hit$to_dict()))
})
# getTableAsTibble ####
setMethod("getTableAsTibble", "PythonDbComm", function(dbComm, tableName, searchBlock = list()) {
  recs <- getTableAsRecords(dbComm, tableName, searchBlock)
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
#' @rdname DbComm-methods
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
  tryCatch({
    ubq <- dbComm@dsl$UpdateByQuery(using = dbComm@client, index = tableName)$
      update_from_dict(list(query = list(term = rlang::list2(!!field := oldValue))))$
      script(source=glue("ctx._source.{field} = params.newValue"), params = list(newValue = newValue))
    resp <- ubq$execute()
  },
  error = function(cnd) {
    warning("error in replaceValueInField: ", conditionMessage(cnd))
  }
  )
  if (resp$success())
    message("Change accepted, ", resp$updated, " documents updated")
  refreshTable(dbComm, tableName)
})
# show ####
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
