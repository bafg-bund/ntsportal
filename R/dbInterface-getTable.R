
#' Retrieve a table from NTSPortal using Query DSL
#' @description Works similarly to `getTableAsRecords()` but reformats the `ntspRecords` into a `tibble`.
#' @inheritParams getTableAsRecords
#' @details The records can take a long time to reformat, use the searchBlock and fields arguments to 
#' refine the search as much as possible thereby reducing the size of the data.
#' @return A tibble
#' @seealso \link{`getTableAsRecords`}
#' @seealso \link{`getTableByEsql`}
#' @export
getTableByQuery <- function(tableName, searchBlock = list(), fields = "*", sortField = "no-sort") {
  dbComm <- getDbComm()
  recs <- getTableAsRecords(dbComm, tableName, searchBlock, fields = fields)
  progBar <- cli_progress_bar("Reformating to tibble", total = length(recs))
  convertRecordsToTibble(recs, progBar)
}

convertRecordsToTibble <- function(recs, progBar = cli_progress_bar()) {
  allFields <- getAllFieldsInRecs(recs)
  unitaryFields <- getUnitaryFields(allFields, recs)
  nestedFields <- getNestedFields(allFields)
  tibbleRowsList <- map(recs, \(rec) {
    cli_progress_update(id = progBar)
    getTibbleRow(rec, unitaryFields, nestedFields)
  })
  list_rbind(tibbleRowsList)
} 

getAllFieldsInRecs <- function(recs) {
  reduce(map(recs, names), union)
}
getUnitaryFields <- function(fields, recs) {
  fieldLengths <- map(recs, \(rec) map_int(fields, \(f) length(rec[[f]])))
  fields[pmap_int(fieldLengths, max) == 1]
}
getNestedFields <- function(fields) {
  fields[fields %in% getAllNestedFields()]
}
getTibbleRow <- function(rec, unitaryFields, nestedFields) {
  if (length(nestedFields) > 0)
    rec <- convertNestedFieldsToTibble(rec, nestedFields)
  unclass(rec) |> 
    tibble::enframe() |> 
    tidyr::pivot_wider() |> 
    tidyr::unnest(cols = any_of(unitaryFields))
}

convertNestedFieldsToTibble <- function(rec, nestedFields) {
  for (field in nestedFields)
    rec[[field]] <- list_rbind(map(rec[[field]], as_tibble))
  rec
}

viewTable <- function(tableName) {
  View(getTableByQuery(tableName))
}

#' Use ES|QL to retrieve a table from NTSPortal
#' @description ES|QL can be prepared in the "Discover" page of NTSPortal and 
#' @param esql ES|QL string
#' @export
#' @details ES|QL does not work for nested fields, fields such as ms2, rtt will be ignored 
#' @examples
#' \dontrun{
#' getTableByEsql("FROM ntsp25.1_dbas* | WHERE station == \"mosel_139\" | KEEP mz | LIMIT 10")
#' }
#' 
getTableByEsql <- function(esql) {
  dbComm <- getDbComm()
  responseBody <- dbComm@client$esql$query(query=esql)$body
  colNames <- map_chr(responseBody$columns, \(x) x$name)
  newTbl <- list_rbind(
    map(responseBody$values, \(x) {
      names(x) <- colNames
      xConcatenated <- map(x, \(el) ifelse(length(el) == 1, el, ifelse(is.null(el), NA, paste(el, collapse = ", "))))
      as_tibble_row(xConcatenated)
    })
  )
  newTbl
}

