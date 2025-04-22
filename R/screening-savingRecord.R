

saveRecord <- function(ntspList, saveDir, maxSizeGb = 10) {
  
  # If featureRecords is too large, cannot write this to json (exceeds R's string
  # object limit)
  # It is estimated that a featureRecord list of >10 GB will create a string that is
  # too large for R to hold.
  
  # Get the memory size in GB
  ntsplSize <- round(as.numeric(object.size(ntspList)) / 1000000000, 1)
  
  # How many times is the object bigger that maxSizeGb, we have to split the object
  # by this number and each part has to be saved as a separate json
  if (ntsplSize > maxSizeGb) {
    # At this point, part should not be in the attributes, otherwise something
    # went wrong
    stopifnot(!is.element("part", names(attributes(ntspList))))
    # How many splits needed
    splitBy <- ceiling(ntsplSize / maxSizeGb)
    splitNames <- letters[1:splitBy]
    splitFac <- rep(splitNames, length.out = length(ntspList))
    # We have to add the part name and the old attributes back to each part
    prevAttributes <- attributes(ntspList)
    prevClass <- class(ntspList)
    # Split up the list and overwrite the memory location
    ntsplListSplit <- split(ntspList, splitFac)
    rm(ntspList)
    ntsplListSplit <- mapply(
      function(theList, thePart, theAtt) {
        attributes(theList) <- theAtt
        attr(theList, "part") <- thePart
        theList
      }, 
      ntsplListSplit, 
      splitNames, 
      MoreArgs = list(theAtt = prevAttributes),
      SIMPLIFY = FALSE
    )
    # for each of these parts, run this function again
    savenm <- lapply(ntsplListSplit, saveRecord, saveDir = saveDir, maxSizeGb = maxSizeGb)
    return(as.character(savenm))
  } 
  
  # If the "part" is not yet set, this means the list is the only part so it 
  # can be set to "a" 
  
  if (!is.element("part", names(attributes(ntspList))))
    attr(ntspList, "part") <- "a"
  
  fileName <- makeFileNameForBatch(ntspList)
  filePath <- file.path(saveDir, fileName)
  
  log_info("Writing JSON file {filePath}")
  writeRecord(ntspList, filePath)
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
  tryCatch({
    jsonString <- rjson::toJSON(record, indent = 2)
    writeLines(jsonString, filePath)
  },
  error = function(cnd) {
    log_error("In writing json {filePath} returned: {conditionMessage(cnd)}")
  })
}

compressJson <- function(filePath) {
  system2("gzip", filePath)
  paste0(filePath, ".gz")
}

uncompressJson <- function(filePath) {
  system2("gunzip", filePath)
  stringr::str_match(filePath, "(.*)\\.gz$")[,2]
}


# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
