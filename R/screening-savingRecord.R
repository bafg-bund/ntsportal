
saveRecord <- function(featureRecordList, saveDir) {
  fileName <- makeFileNameForBatch(featureRecordList)
  filePath <- file.path(saveDir, fileName)
  writeRecord(featureRecordList, filePath)
  log_info("Completed RDS file {filePath}")
  filePath
} 

makeFileNameForBatch <- function(featureRecords) {
  dirName <- dirname(featureRecords[[1]][["path"]])
  batchNameHash <- digest::digest(dirName, algo = "crc32")
  paste0(
    "ntsportal-featureRecord-", 
    format(Sys.time(), "%y%m%d-%H%M-"), 
    batchNameHash,
    ".RDS"
  )
}

writeRecord <- function(record, filePath) {
  log_info("Writing RDS file {filePath}")
  tryCatch(
    saveRDS(record, filePath),
    error = function(cnd) log_error("In writing RDS {filePath} returned: {conditionMessage(cnd)}")
  )
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
