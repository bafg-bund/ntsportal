


#' Change msrawfiles to a new version
#'
#' @param msrawfilesName Name of msrawfiles table
#' @param version New version number
#' @param dbCommGenerator Function to produce a new connection interface 
#'
#' @export
#'
#' @examples
#' \dontrun{
#' msrawfilesSetVersion("msrawfiles", "25.1")
#' }
msrawfilesSetVersion <- function(msrawfilesName, version, dbCommGenerator = PythonDbComm) {
  version <- as.character(version)
  verifyVersionText(version)
  newTableName <- getNewTableName(msrawfilesName, version)
  dbComm <- dbCommGenerator()
  copyTable(dbComm, msrawfilesName, newTableName, "msrawfiles")
  changeAllDbasAliasNames(dbComm, newTableName, version)
}

verifyVersionText <- function(version) {
  versionRegex <- "^\\d\\d\\.\\d$"
  currentYear <- format(Sys.Date(), "%y")
  if (!is.character(version) || !grepl(versionRegex, version) || substr(version, 1, 2) != currentYear)
    stop("Version number must be of the form 'YY.X' with YY being the current year")
}

getNewTableName <- function(oldName, version) {
  stringr::str_replace(oldName, "^(ntsp)\\d?\\d?\\.?\\d?(_.*)$", glue("\\1{version}\\2"))
}

changeAllDbasAliasNames <- function(dbComm, msrawfilesName, version) {
  allAliases <- getAllDbasAliasNames(dbComm, msrawfilesName)
  newAliases <- getNewTableName(allAliases, version)
  walk2(allAliases, newAliases, changeDbasAliasName, dbComm = dbComm, msrawfilesName = msrawfilesName)
} 

getAllDbasAliasNames <- function(dbComm, msrawfilesName) {
  resp <- dbComm@client$search(
    index = msrawfilesName, 
    body = list(aggs = list(aliases = list(terms = list(field = "dbas_alias_name", size = 1000))))
  )
  map_chr(resp$body$aggregations$aliases$buckets, function(x) x$key)
}

changeDbasAliasName <- function(dbComm, msrawfilesName, oldName, newName) {
  tryCatch({
    ubq <- dbComm@dsl$UpdateByQuery(using = dbComm@client, index = msrawfilesName)$
      query("term", dbas_alias_name = oldName)$
      script(source="ctx._source.dbas_alias_name = params.newName", params = list(newName = newName))
    resp <- ubq$execute()
    },
    error = function(cnd) {
      warning("error in changeDbasAliasName: ", conditionMessage(cnd))
    }
  )
  if (resp$success())
    message("Change accepted")
  refreshTable(dbComm, msrawfilesName)
}








