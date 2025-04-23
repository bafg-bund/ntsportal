

saveRecord <- function(featureRecordList, saveDir, maxSizeGb = 10) {
  
  ntsplSize <- getObjectSizeGB(featureRecordList)
  
  # If featureRecords is too large, cannot write this to json (exceeds R's string object limit) It is estimated that a
  # featureRecord list of >10 GB will create a string that is too large for R to hold. we have to split the object and
  # each part has to be saved as a separate json
  if (ntsplSize > maxSizeGb) {
    stopifnot(noPartAttribute(featureRecordList))
    splitBy <- ceiling(ntsplSize / maxSizeGb)
    splitFac <- rep(1:splitBy, length.out = length(featureRecordList))
    featureRecordListSplit <- split(featureRecordList, splitFac)
    rm(featureRecordList)
    for (i in seq_along(featureRecordListSplit)) {
      attr(featureRecordListSplit[[i]], "part") <- letters[i]
    }
    # Run this function on each part
    savenm <- lapply(featureRecordListSplit, saveRecord, saveDir = saveDir, maxSizeGb = maxSizeGb)
    return(as.character(savenm))
  } 
  
  # If the "part" is not set this is the only part.
  if (noPartAttribute(featureRecordList))
    attr(featureRecordList, "part") <- "a"
  
  fileName <- makeFileNameForBatch(featureRecordList)
  filePath <- file.path(saveDir, fileName)
  writeRecord(featureRecordList, filePath)
  newFilePath <- compressJson(filePath)
  
  log_info("Completed JSON file {newFilePath}")
  newFilePath
} 

makeFileNameForBatch <- function(featureRecords) {
  stopifnot(is.character(attr(featureRecords, "part")))
  stopifnot(nchar(attr(featureRecords, "part")) == 1)
  stopifnot(attr(featureRecords, "part") %in% letters)
  dirName <- dirname(featureRecords[[1]][["path"]])
  batchNameHash <- digest::digest(dirName, algo = "crc32")
  paste0(
    "ntsportal-featureRecord-", 
    format(Sys.time(), "%y%m%d-%H%M-"), 
    batchNameHash,
    "-part-",
    attr(featureRecords, "part"),
    ".json"
  )
}

writeRecord <- function(record, filePath) {
  log_info("Writing JSON file {filePath}")
  tryCatch({
    jsonString <- rjson::toJSON(record, indent = 2)
    writeLines(jsonString, filePath)
  },
  error = function(cnd) {
    log_error("In writing json {filePath} returned: {conditionMessage(cnd)}")
  })
}

compressJson <- function(filePath) {
  newName <- paste0(filePath, ".gz")
  if (file.exists(newName))
    stop("A file with the name ", newName, "already exists. Aborting.")
  system2("gzip", filePath)
  newName
}

uncompressJson <- function(filePath) {
  system2("gunzip", filePath)
  stringr::str_match(filePath, "(.*)\\.gz$")[,2]
}

getObjectSizeGB <- function(featureRecordList) {
  round(as.numeric(object.size(featureRecordList)) / 1000000000, 1)
}

noPartAttribute <- function(featureRecordList) {
  !is.element("part", names(attributes(featureRecordList)))
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
