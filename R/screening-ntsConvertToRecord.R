
#' Convert a result of NTS processing to a list of feature records
#' @description The results of NTS file scanning (`ntsResult`) are converted to the NTSPortal `featureRecord` format.
#' Annotated features (matching a substance in the spectral library) are removed. 
#' @rdname convertToRecord
#' @returns a `list` of `featureRecord`s
#' @details
#' This method does not use `msrawfilesBatch` but it needs to be in the method definition for polymorphism, 
#' e.g. in `screeningOneBatch()`.
#' @examples
#' \dontrun{
#'   testMsrawfilesRecords <- getRecordsSampleAndBlank()
#'   testNtsResults <- scanBatch(testMsrawfilesRecords)
#'   features <- convertToRecord(testNtsResults)  
#' }
#' @export
convertToRecord.ntsResult <- function(scanResult, msrawfilesBatch = NA) {
  scanResult <- removeAnnotated(scanResult)
  scanResult <- removeBlankFiles(scanResult)
  
  features <- getAreasOfFeatures(scanResult)
  features <- addIntStdToFeatures(scanResult, features)
  features <- addAliasToFeatures(scanResult, features)
  features <- addSpectraToFeatures(scanResult, features)
  features <- cleanUpFeatures(features)
  features
}

removeAnnotated <- function(scanResult) {
  ids <- scanResult$annotationTable |> pull(alignmentId)
  scanResult$alignmentTable <- scanResult$alignmentTable |> filter(!is.element(alignmentId, ids))
  scanResult
} 

removeBlankFiles <- function(scanResult) {
  scanResult$sampleList <- filter(scanResult$sampleList, sampleType == "Unknown")
  sampleIdsUnknowns <- scanResult$sampleList[, "sampleId", drop = T]
  scanResult$peakList <- filter(scanResult$peakList, sampleId %in% sampleIdsUnknowns)
  scanResult$alignmentTable <- filter(scanResult$alignmentTable, sampleId %in% sampleIdsUnknowns)
  scanResult$annotationTable <- filter(scanResult$annotationTable, alignmentId %in% scanResult$alignmentTable$alignmentId)
  scanResult
}

#' @export
getAreasOfFeatures.ntsResult <- function(scanResult) {
  samp <- scanResult$sampleList[, c("sampleId", "path")]
  alig <- merge(scanResult$alignmentTable, samp, by = "sampleId", all.x = T)
  plArea <- scanResult$peakList[, c("area", "peakId")]
  alig <- merge(alig, plArea, by = "peakId", all.x = TRUE, sort = FALSE)
  features <- split(alig, seq_len(nrow(alig)))
  unname(lapply(features, as.list))
}

#' @export
addIntStdToFeatures.ntsResult <- function(scanResult, features) {
  stdDf <- select(scanResult$sampleList, path, normalizePeakId, internal_standard = intStdName)
  peakIds <- stdDf$normalizePeakId
  stdDf$area_internal_standard <- map_dbl(peakIds, \(x) getResponse(x, scanResult$peakList, "area"))
  stdDf$intensity_internal_standard <- map_dbl(peakIds, \(x) getResponse(x, scanResult$peakList, "intensity"))
  stdList <- split(stdDf, stdDf$path)
  stdList <- map(stdList, \(x) select(x, -path, -normalizePeakId) |> as.list())
  map(features, \(rec) c(rec, stdList[[rec$path]]))
}

addAliasToFeatures <- function(scanResult, features) {
  stdDf <- select(scanResult$sampleList, path, feature_table_alias = featureAliasName)
  stdList <- split(stdDf, stdDf$path)
  stdList <- map(stdList, \(x) select(x, -path) |> as.list())
  map(features, \(rec) c(rec, stdList[[rec$path]]))
}


addSpectraToFeatures <- function(scanResult, features) {
  UseMethod("addSpectraToFeatures")
}

#' @export
addSpectraToFeatures.ntsResult <- function(scanResult, features) {
  features <- features[order(map_chr(features, \(x) x$path))]
  currentMeasFile <- openMeasFile(features[[1]]$path)
  for (featNum in seq_along(features)) {
    featPeakId <- features[[featNum]]$peakId
    if (!isMeasFile(currentMeasFile, features[[featNum]]))
      currentMeasFile <- openMeasFile(features[[featNum]]$path)
    features[[featNum]]$eic <- getEicFromScanResult(scanResult, featPeakId, measFile = currentMeasFile, rec = features[[featNum]])
    features[[featNum]]$ms1 <- getMs1FromScanResult(scanResult, featPeakId, measFile = currentMeasFile, rec = features[[featNum]])
    features[[featNum]]$ms2 <- getMs2FromScanResult(scanResult, featPeakId, measFile = currentMeasFile, rec = features[[featNum]])
  }
  features
}

#' @export
getEicFromScanResult.ntsResult <- function(scanResult, peakId, measFile, rec) {
  eic <- getRawEicFromMeasFile(measFile, scanResult, peakId, rec)
  if (exists("eic") && length(eic) > 0) {
    eic$time <- getScanTimeEic(eic, measFile)  
    eic <- reduceRowsEic(eic)
    eicm <- eicAsMatrix(eic)
    eicm <- cleanUpEic(eicm)
    if (nrow(eicm) > 4) {
      matrixToList(eicm, c("time", "int"))
    } 
  } 
}

getRawEicFromMeasFile <- function(measFile, scanResult, peakId, rec) {
  pl <- scanResult$peakList
  eicExtractionWidth <- filter(scanResult$sampleList, path == rec$path) |> pull(eicExtractionWidth)
  leftEndRt <- pl[pl$peakId == !!peakId, "leftEndRt", drop = T] - 20
  rightEndRt <- pl[pl$peakId == !!peakId, "rightEndRt", drop = T] + 20
  tryCatch(
    xcms::rawEIC(
      measFile,
      mzrange = c(rec$mz - eicExtractionWidth / 2, rec$mz + eicExtractionWidth / 2),
      rtrange = c(leftEndRt, rightEndRt)
    ),
    error = function(cnd) {
      log_warn("Error in EIC extraction in file: {rec$path}, PeakID: {rec$peakId}. message: {conditionMessage(cnd)}")
      list()
    }
  )
}

getScanTimeEic <- function(eic, measFile) {
  tryCatch(
    round(measFile@scantime[eic$scan], 4),  # eic.time in s
    error = function(cnd) {
      log_warn("Error in getScanTimeEic for file {measFile@filepath}")
      rep(0, length(eic$scan))
    }
  )
}

reduceRowsEic <- function(eic) {
  reduction <- floor(length(eic$time) / 100)
  if (reduction > 1) {
    oo <- zoo::zoo(cbind(eic$time, eic$int), eic$scan)
    oor <- zoo::rollapply(oo, reduction, mean, by = reduction)
    list(time = as.numeric(oor[,1]), int = as.numeric(oor[,2]))
  } else {
    eic
  }
}

eicAsMatrix <- function(eic) {
  cbind(time = eic$time, int = eic$int)
}

cleanUpEic <- function(eic) {
  eic <- eic[eic[, "int"] > 0, , drop = FALSE]
  eic[eic[, "int"] > 0, , drop = FALSE]
}

#' @export
getMs1FromScanResult.ntsResult <- function(scanResult, peakId, measFile, rec) {
  ms1scan <- scanResult$peakList |> filter(peakId == !!peakId) |> slice(1) |> pull(ms1scan)
  ms1 <- getMs1ScanFromMeasFile(measFile, ms1scan)
  if (nrow(ms1) > 0) {
    ms1 <- cleanUpMs1(ms1, rec, scanResult)
    if (nrow(ms1) > 0) {
      dfToList(ms1)
    }
  }
}

cleanUpMs1 <- function(ms1, rec, scanResult) {
  colnames(ms1) <- c("mz", "int")
  ms1 <- ms1[ms1$mz > rec$mz - 1 & ms1$mz < rec$mz + 5, , drop = F]  # Focus only on area of spec around mz of feature
  noiseLevel <- scanResult$peakList |> filter(peakId == rec$peakId) |> slice(1) |> pull(baseline)
  ms1 <- removeNoiseMsSpec(ms1, noiseLevel)
  ms1 <- convertSpecToRelativeIntensities(ms1, rec$intensity)
  limitSizeMsSpec(ms1)
}

#' @export
getMs2FromScanResult.ntsResult <- function(scanResult, peakId, measFile, rec) {
  ms2scan <- scanResult$peakList |> filter(peakId == !!peakId) |> slice(1) |> pull(ms2scan)
  if (ms2scan == 0)
    return(NULL)
  ms2 <- getMs2ScanFromMeasFile(measFile, ms2scan)
  if (nrow(ms2) > 0) {
    ms2 <- cleanUpMs2(ms2, rec)
    if (nrow(ms2) > 0) {
      return(dfToList(ms2))
    } else {
      return(NULL)
    }
  }
}

cleanUpMs2 <- function(ms2, rec) {
  colnames(ms2) <- c("mz", "int")
  ms2 <- ms2[ms2$mz < rec$mz + 1, , drop = F]
  ms2 <- limitSizeMsSpec(ms2)
  if (nrow(ms2) > 0)
    convertSpecToRelativeIntensities(ms2, max(ms2$int)) else getEmptySpectrum()
}

getEmptySpectrum <- function() {
  tibble(mz = numeric(), int = numeric())
}

getMs1ScanFromMeasFile <- function(measFile, ms1scan) {
  tryCatch(
    as_tibble(xcms::getScan(measFile, ms1scan)),
    error = function(cnd) {
      log_warn("Error in getMs1ScanFromMeasFile for file: {measFile@filepath}. message: {conditionMessage(cnd)}")
    }
  )
}

getMs2ScanFromMeasFile <- function(measFile, ms2scan) {
  tryCatch(
    as_tibble(xcms::getMsnScan(measFile, ms2scan)),
    error = function(cnd) {
      log_warn("Error in getMs2ScanFromMeasFile for file: {measFile@filepath}. message: {conditionMessage(cnd)}")
    }
  )
}


matrixToList <- function(mat, colNames) {
  l <- split(mat, seq_len(nrow(mat)))
  l <- lapply(l, as.list)
  l <- map(l, \(x) {names(x) <- colNames; x})
  unname(l)
}

isMeasFile <- function(measFile, featRecord) {
  measFile@filepath == featRecord$path
}

openMeasFile <- function(pth) {
  suppressMessages(xcms::xcmsRaw(pth, includeMSn = T))
}

getResponse <- function(peakId, pl, responseType) {
  response <- pl[pl$peakId == peakId, responseType, drop = TRUE]
  ifelse(is.null(response), NA, response)
}

#' @export
addIntStdToFeatures <- function(scanResult, features) {
  UseMethod("addIntStdToFeatures")
}

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
