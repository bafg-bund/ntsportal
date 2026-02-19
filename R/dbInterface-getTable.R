
#' Retrieve a table from NTSPortal using Query DSL
#' @description Works similarly to `getTableAsRecords()` but reformats the `ntspRecords` into a `tibble`.
#' @inheritParams getTableAsRecords
#' @details The records can take a long time to reformat, use the searchBlock and fields arguments to 
#' refine the search as much as possible thereby reducing the size of the data. Nested fields are coerced
#' to list columns of tibbles. Array fields are coerced to character columns with elements concatenated 
#' with pipe "|"
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
  nestedFields <- getNestedFields(allFields)
  arrayFields <- getArrayFields(setdiff(allFields, nestedFields), recs)
  tibbleRowsList <- map(recs, \(rec) {
    cli_progress_update(id = progBar)
    getTibbleRow(rec, nestedFields, arrayFields)
  })
  list_rbind(tibbleRowsList)
} 

getAllFieldsInRecs <- function(recs) {
  reduce(map(recs, names), union)
}

getNestedFields <- function(fields) {
  fields[fields %in% getAllNestedFields()]
}

getArrayFields <- function(fields, recs) {
  fieldLengths <- map(recs, \(rec) map_int(fields, \(f) length(rec[[f]])))
  fields[pmap_int(fieldLengths, max) > 1]
}

getTibbleRow <- function(rec, nestedFields, arrayFields) {
  if (length(nestedFields) > 0) {
    recNoNested <- rec[-which(names(rec) %in% nestedFields)]
    recNoNested <- arrayFieldsToList(recNoNested, arrayFields)
    rowNoNested <- unclass(recNoNested) |> as_tibble_row()
    rowNested <- convertNestedFieldsToTibble(rec, nestedFields)
    cbind(rowNoNested, rowNested)
  } else {
    rec <- arrayFieldsToList(rec, arrayFields)
    unclass(rec) |> as_tibble_row()
  }
}

arrayFieldsToList <- function(rec, arrayFields) {
  for (field in arrayFields)
    rec[[field]] <- list(rec[[field]])
  rec
}

convertNestedFieldsToTibble <- function(rec, nestedFields) {
  newRec <- list()
  for (field in nestedFields)
    newRec[[field]] <- list(list_rbind(map(rec[[field]], as_tibble)))
  tibble::tibble_row(!!!newRec)
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
    test <- map(responseBody$values, \(x) {
      names(x) <- colNames
      xConcatenated <- map(
        x, 
        \(el) ifelse(is.null(el), NA, ifelse(length(el) == 1, el, paste(el, collapse = ", ")))
      )
      as_tibble_row(xConcatenated)
    })
  )
  newTbl
}

