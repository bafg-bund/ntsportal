



prepareExampleFeatureIndex <- function(ntspVersion) {
  removeExampleFeatureIndex(ntspVersion)
  emptyResult <- convertToDbasResult(emptyReport())
  emptyRecord <- convertToRecord(emptyResult, list(getRecordNoPeaks()))
  tempDir <- withr::local_tempdir()
  saveRecord(emptyRecord, tempDir)
  indexNames <- ingest(tempDir)
  dbComm <- getDbComm()
  refreshTable(dbComm, indexNames[[1]][[1]])
}

removeExampleFeatureIndex <- function(ntspVersion) {
  dbComm <- getDbComm()
  tableNames <- getAliasTable(dbComm, glue("ntsp{ntspVersion}_dbas_unit_tests"))
  
  if (tableNames[1] == "")
    return(NULL)
  
  for (tableName in tableNames) 
    deleteTable(dbComm, tableName)
}
