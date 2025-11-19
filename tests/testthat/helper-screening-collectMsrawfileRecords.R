



prepareExampleFeatureIndex <- function(ntspVersion) {
  removeExampleFeatureIndex(ntspVersion)
  emptyResult <- convertToDbasResult(emptyReport())
  emptyRecord <- convertToRecord(emptyResult, list(getMsrawfilesRecordNoPeaks("dbas")))
  tempDir <- withr::local_tempdir()
  saveRecord(emptyRecord, tempDir)
  indexNames <- ingestFeatureRecords(tempDir)
}

removeExampleFeatureIndex <- function(ntspVersion) {
  dbComm <- getDbComm()
  tableNames <- getAliasTable(dbComm, glue("ntsp{ntspVersion}_feature_unit_tests"))
  
  if (tableNames[1] == "")
    return(NULL)
  
  for (tableName in tableNames) 
    deleteTable(dbComm, tableName)
}
