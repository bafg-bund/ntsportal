

#' Create a backup of an msrawfiles index
#' @description Will create a backup index with the current date in the name (`<msrawfilesName>_backup_<YYYYMMDD>`). It will not
#' overwrite an index unless overwrite = TRUE. The backup index is immediately closed so that it does not interfere.
#' @param msrawfilesName Name of index to back up
#' @param overwrite Logical, should an existing index be overwritten (default is false)
#' @return Returns the name of the new table
#' @export
createBackupMsrawfiles <- function(msrawfilesName, overwrite = FALSE) {
  dbComm <- getDbComm()
  backupName <- makeBackupName(msrawfilesName)
  if (isTable(dbComm, backupName) && overwrite)
    deleteTable(dbComm, backupName)
  if (isTable(dbComm, backupName))
    stop("Cannot create backup, ", backupName, " already exists.")
  copyTable(
    dbComm, 
    msrawfilesName, 
    backupName, 
    "msrawfiles"
  )
  closeTable(dbComm, backupName)
  backupName
}

makeBackupName <- function(tableName) {
  paste0(tableName, "_backup_", format(Sys.Date(), "%Y%m%d"))
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal