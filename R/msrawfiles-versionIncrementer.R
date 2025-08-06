
#' Change msrawfiles to a new version
#' @description To move to a new version of NTSPortal, `msrawfiles` needs to be updated with
#' new alias names.
#' @param msrawfilesName Name of `msrawfiles` table
#' @param version New version number
#' @export
#' @examples
#' \dontrun{
#' msrawfilesSetVersion("ntsp25.1_msrawfiles", "25.2")
#' }
msrawfilesSetVersion <- function(msrawfilesName, version) {
  version <- as.character(version)
  verifyVersionText(version)
  newTableName <- getNewTableName(msrawfilesName, version)
  dbComm <- getOption("ntsportal.dbComm")()
  copyTable(dbComm, msrawfilesName, newTableName, "msrawfiles")
  changeAllDbasAliasNames(newTableName, version)
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

changeAllDbasAliasNames <- function(msrawfilesName, version) {
  dbComm <- getOption("ntsportal.dbComm")()
  allAliases <- getUniqueValues(dbComm, msrawfilesName, "dbas_alias_name")
  newAliases <- getNewTableName(allAliases, version)
  walk2(allAliases, newAliases, 
    function(old, new) 
      replaceValueInField(
        dbComm = dbComm, 
        tableName = msrawfilesName, 
        field = "dbas_alias_name",
        oldValue = old,
        newValue = new
      )
  )
} 

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
