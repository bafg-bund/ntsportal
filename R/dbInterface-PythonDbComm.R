


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



#' @rdname DbComm-methods
#' @aliases ping,PythonDbComm-method
setMethod("ping", "PythonDbComm", function(dbComm) {
  dbComm@client$ping()
})


setMethod("refreshTable", "PythonDbComm", function(dbComm, tableName) {
  invisible(dbComm@dsl$Index(tableName)$refresh(using=dbComm@client))
})


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


setMethod("isTable", "PythonDbComm", function(dbComm, tableName) {
  dbComm@dsl$Index(tableName)$exists(using = dbComm@client)
})


setMethod("createNewTable" , "PythonDbComm", function(dbComm, tableName, mappingType) {
  mapping <- getMapping(mappingType)
  dbComm@client$indices$create(index = tableName, body = mapping)
})


setMethod("copyTable" , "PythonDbComm", function(dbComm, tableName, newTableName, mappingType) {
  createNewTable(dbComm, newTableName, mappingType)
  dbComm@client$reindex(source = list(index = tableName), dest = list(index = newTableName))
  refreshTable(dbComm, newTableName)
})


setMethod("getUniqueValues", "PythonDbComm", function(dbComm, tableName, field, maxLength = 10000) {
  resp <- dbComm@client$search(
    index = tableName, 
    body = list(aggs = list(aliases = list(terms = list(field = field, size = maxLength))))
  )
  map_chr(resp$body$aggregations$aliases$buckets, function(x) x$key)
})

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

setMethod("getTableAsRecords", "PythonDbComm", function(dbComm, tableName, queryBlock = list(), recordConstructor = newNtspRecord) {
  queryBlock <- matchAllIfEmpty(queryBlock)
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(queryBlock)
  if (s$count() > 1e6)
    stop("Exceeded the maximum docs that may be retrieved (1e6)")
  iterate(s$iterate(), function(hit) recordConstructor(hit$to_dict()))
})

getTableAsTibble <- function(dbComm, tableName, queryBlock = list()) {
  recs <- getTableAsRecords(dbComm, tableName, queryBlock)
  convertRecordsToTibble(recs)
}

convertRecordsToTibble <- function(recs) {
  tibbleRowsList <- map(recs, getTibbleRow)
  dplyr::bind_rows(tibbleRowsList)
} 

getTibbleRow <- function(rec) {
  tibbleRow <- unclass(rec) |> 
    tibble::enframe() |> 
    tidyr::pivot_wider() 
  
  length1Cols <- tibbleRow |>
    select(where(is.list)) |>
    select(where(~ all(lengths(.) == 1))) |>
    names()
  
  tibbleRow |> tidyr::unnest(cols = all_of(length1Cols))
}

setMethod("getNrow", "PythonDbComm",function(dbComm, tableName, queryBlock = list()) {
  queryBlock <- matchAllIfEmpty(queryBlock)
  
  s <- dbComm@dsl$Search(using=dbComm@client, index = tableName)$
    update_from_dict(queryBlock)
  s$count()
})

setMethod("deleteRow", "PythonDbComm", function(dbComm, tableName, queryBlock) {
  resp <- dbComm@client$delete_by_query(index = tableName, body = queryBlock)
  message(resp$body$deleted, " row(s) deleted")
})

setMethod("getAliasTable" , "PythonDbComm",  function(dbComm, aliasName) {
  stopifnot(length(aliasName) == 1)
  resp <- dbComm@client$cat$aliases(name=aliasName)
  if (resp$body == "")
    return("")
  respTab <- read.delim(text = resp$body, header = F, sep = " ") 
  respTab[, 2]
})

setMethod("appendRecords", "PythonDbComm", function(dbComm, tableName, records) {
  pyIngestModule$appendRecordsToTable(tableName, records, dbComm@client)
  refreshTable(dbComm, tableName)
  message("Records appended to table ", tableName)
})


matchAllIfEmpty <- function(queryBlock) {
  if (length(queryBlock) == 0) {
    return(list(query = list(match_all = stats::setNames(list(), character(0)))))
  } else {
    return(queryBlock)
  }
}
