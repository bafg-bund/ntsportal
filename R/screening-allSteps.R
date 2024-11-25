
#' @export
dbaScreeningNewBatches <- function(msrawfileIndex, saveDirectory) {
  recordsInBatches <- getUnprocessedMsrawfileRecords(msrawfileIndex, "dbas")
  dbaScreeningBatches(recordsInBatches, saveDirectory)
}

#' @export
dbaScreeningSelectedBatches <- function(msrawfileIndex, batchDirs, saveDirectory) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  dbaScreeningBatches(recordsInBatches, saveDirectory)
}

dbaScreeningBatches <- function(msrawfileRecordsInBatches, saveDirectory) {
  checkQualityBatchList(msrawfileRecordsInBatches)
  purrr::map_chr(msrawfileRecordsInBatches, dbaScreeningOneBatch, saveDirectory = saveDirectory)
}

dbaScreeningOneBatch <- function(msrawfileRecords, saveDirectory) {
  resultBatch <- scanBatchDbas(msrawfileRecords)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfileRecords)
  saveRecord(featureRecordsBatch, saveDirectory)
}


