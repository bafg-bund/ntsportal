

#' Create a backup of an msrawfiles index
#' 
#' Will create a backup index with the current date in the name (<msrawfilesName>_backup_<YYYYMMDD>). It will not
#' overwrite an index unless overwrite = TRUE. The backup index is immediately closed so that it does not interfere.
#' 
#' @param msrawfilesName name of index to back up
#' @param overwrite logical, should an existing index be overwritten (default is false)
#' 
#' @return returns the name of the new table
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
