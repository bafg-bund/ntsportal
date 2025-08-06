
#' Scan measurement files for known compounds
#' @description The spectral library is used to scan mzXML measurment files for compounds using the DBAS algorithm.
#' The parameters for DBAS and file locations (measurement files and spectral library) are stored in the `msrawfileRecord`s 
#' passed to the `records` argument. 
#' @param records `list` of `msrawfileRecord`s
#' @param compsToProcess Character vector of compounds names to include in the scanning (default is all compounds in the spectral library)
#' @param showProgress Logical, show progress bar? (for interacte use, default false)
#' @returns a `dbasResult` (`list` of 7 tables) including `peakList`, `reintegrationResults` etc.
#' @export
#' @examples
#' \dontrun{
#' dbComm <- getDbComm()
#' recs <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.2_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(filename = "Des_19_.._pos.mzXML"))),
#'   newMsrawfilesRecord
#' )
#' recsBlanks <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.2_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(path = ".*mud_pos/BW.*"))), 
#'   newMsrawfilesRecord
#' )
#' res <- scanBatchDbas(c(recs, recsBlanks), "Methyltriphenylphosphonium")
#' }
scanBatchDbas <- function(records, compsToProcess = NULL, showProgress = FALSE) {
  progBar <- ifelse(showProgress, cli_progress_bar("Processing batch", total = length(records)), "no-progress")
  reports <- purrr::map(records, fileScanDbas, compsToProcess = compsToProcess, progBar = progBar)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  cleanedReport <- cleanReport(mergedReport, records)
  reintegratedReport <- reintegrateReport(cleanedReport)
  dbasResults <- convertToDbasResult(reintegratedReport)
  dbasResults
}

fileScanDbas <- function(msrawfileRecord, compsToProcess = NULL, progBar = "no-progress") {
  fileScannerDbas <- createScannerDbas(msrawfileRecord)
  fileScannerDbas <- runScanningDbas(fileScannerDbas, compsToProcess)
  if (grepl("^cli-", progBar)) cli_progress_update(id = progBar)
  fileScannerDbas
}

createScannerDbas <- function(msrawfileRecord) {
  rec <- msrawfileRecord
  scanner <- ntsworkflow::Report$new()
  scanner$addRawFiles(F, rec$path)
  scanner$addIS(F, rec$dbas_is_table)
  scanner$addDB(F, rec$dbas_spectral_library)
  scanner$changeSettings("blank_int_factor", rec$dbas_blank_int_factor)
  scanner$changeSettings("area_threshold", rec$dbas_area_threshold)
  scanner$changeSettings("rttolm", rec$dbas_rttolm)
  scanner$changeSettings("mztolu", rec$dbas_mztolu)
  scanner$changeSettings("mztolu_fine", rec$dbas_mztolu_fine)
  scanner$changeSettings("use_int_threshold", "area")
  scanner$changeSettings("threshold", rec$dbas_ndp_threshold)
  scanner$changeSettings("rtTolReinteg", rec$dbas_rtTolReinteg)
  scanner$changeSettings("ndp_m", rec$dbas_ndp_m)
  scanner$changeSettings("ndp_n", rec$dbas_ndp_n)
  scanner$changeSettings("instr", unlist(rec$dbas_instr))
  scanner$changeSettings("pol", rec$pol)
  scanner$changeSettings("numcores", 1)
  scanner$changeSettings("chromatography", rec$chrom_method)
  scanner
}


runScanningDbas <- function(scannerObj, compsToProcess) {
  backupScanner <- scannerObj$copy()
  tryCatch({
    suppressMessages(scannerObj$process_all(comp_names = compsToProcess))
    scannerObj$clearData()
    warnNoPeaks(scannerObj)
    scannerObj <- placeholderToSetMockingFunctions(scannerObj)
  }, 
  error = function(cnd) {
    log_error("Processing error in file: {scannerObj$rawFiles},
                  error message: {conditionMessage(cnd)}")
  })
  if (is.null(scannerObj) || inherits(scannerObj, "try-error"))
    backupScanner else scannerObj
}

warnNoPeaks <- function(report) {
  if (nrow(report$peakList) == 0)
    log_warn("No peaks found in file: {report$rawFiles}")
}

placeholderToSetMockingFunctions <- function(x) {
  x
}

removeEmptyReports <- function(reports) {
  Filter(Negate(isEmptyReport), reports)
}

mergeReports <- function(reports) {
  if (length(reports) == 0) {
    emptyReport()
  } else if (length(reports) == 1) {
    reports[[1]]
  } else if (length(reports) > 1) {
    Reduce(ntsworkflow::mergeReport, reports)
  } 
}

cleanReport <- function(report, records) {
  if (!isEmptyReport(report)) 
    report <- blankCorrectionDbas(report, records)
  if (!isEmptyReport(report)) 
    report <- removeFalsePositives(report, records)
  if (!isEmptyReport(report)) 
    report <- removeDuplicateDetections(report)
  report
}

blankCorrectionDbas <- function(report, records) {
  blanks <- areBlanks(report, records)
  if (all(!blanks))
    return(report)
  report$deleteBackground(which(!blanks), which(blanks))
  report$remRawFiles(which(blanks))
  report
}

areBlanks <- function(report, records) {
  paths <- report$rawFiles
  isBlank <- getField(records, "blank")
  names(isBlank) <- getField(records, "path")
  isBlank[paths]
}

removeFalsePositives <- function(report, records) {
  # These fields are uniform in the records so just take the first non blank
  blanks <- getField(records, "blank")
  minimumDetections <- getField(records, "dbas_minimum_detections")[!blanks][1]
  replicateRegex <- getField(records, "dbas_replicate_regex")[!blanks][1]
  
  falsePositives <- bind_rows(
    getFalsePositivesFromRecord(report, records),
    getCompoundsBelowMinimumDetections(report, minimumDetections),
    getCompoundsNoReplicateDetections(report, replicateRegex)
  ) |> distinct() |> tidyr::nest(fileIndices = fileIndex, .by = "compName")
  
  if (nrow(falsePositives) > 0) { # i <- 1
    for (i in 1:nrow(falsePositives)) {
      report$deleteFP(
        pluck(falsePositives, "compName", i),
        pluck(falsePositives, "fileIndices", i, 1)
      )
    }
  }
  report
}
getFalsePositivesFromRecord <- function(report, records) {
  files <- basename(report$rawFiles)  # file index is given by report$rawFiles, index needed for Report$deleteFP
  fpList <- setNames(getField(records, "dbas_fp"), basename(getField(records, "path")))
  if (length(fpList) > 0) {
    fpRows <- map(seq_along(files), function(ind) tibble(fileIndex = ind, compName = fpList[[files[ind]]]))
    bind_rows(fpRows)
  } else {
    return(emptyFalsePostivesTibble())
  }
}

removeDuplicateDetections <- function(report) {
  report$peakList <- report$peakList[report$peakList$peak == "A", ]
  report
}

getCompoundsBelowMinimumDetections <- function(report, minimumDetections) {
  if (minimumDetections > 1) {
    timesFound <- by(report$peakList, report$peakList$comp_name, nrow)
    scarce <- which(timesFound < minimumDetections)
    tidyr::expand_grid(compName = names(scarce), fileIndex = seq_along(report$rawFiles)) 
  } else {
    return(emptyFalsePostivesTibble())
  }
}

getCompoundsNoReplicateDetections <- function(report, replicateRegex) {
  if (is.na(replicateRegex)) return(emptyFalsePostivesTibble())
  pl <- report$peakList
  pl$originalSampName <- stringr::str_replace(pl$samp, replicateRegex, "\\1")
  replicateCount <- by(pl, list(pl$comp_name, pl$originalSampName), nrow) |> array2DF()
  colnames(replicateCount) <- c("compName", "originalSampName", "count")
  replicateCount <- replicateCount[!is.na(replicateCount$count), ]
  if (nrow(replicateCount) == 0) return(emptyFalsePostivesTibble())
  markAsFp <- replicateCount[replicateCount$count < 2, c("compName", "originalSampName")]
  if (nrow(markAsFp) == 0) return(emptyFalsePostivesTibble())
  map2_dfr(
    markAsFp$compName, 
    markAsFp$originalSampName, 
    function(compName, originalSampName) getFileIndices(compName, originalSampName, pl, report$rawFiles)
  )
}

getFileIndices <- function(compName, originalSampName, pl, reportRawFiles) {
  sampNames <- pl[pl$originalSampName == originalSampName, "samp"] |> unique()
  tibble(
    compName = compName,
    fileIndex = which(basename(reportRawFiles) %in% sampNames)
  )
}

emptyFalsePostivesTibble <- function() {
  tibble(compName = character(), fileIndex = numeric())
}
reintegrateReport <- function(report) {
  originalReport <- report$copy()
  batchPath <- dirname(report$rawFiles[1])
  tryCatch({
    suppressMessages(report$reIntegrate())
    report <- placeholderToSetMockingFunctions(report)
  },
  error = function(cnd) {
    log_warn("Error in reintegration of batch {batchPath}. Error text: {conditionMessage(cnd)}")
    report <<- originalReport
  })
  report
}

convertToDbasResult <- function(report) {
  results <- list(
    peakList = report$peakList,
    reintegrationResults = report$integRes,
    rawFilePaths = report$rawFiles,
    ms1Table = report$MS1,
    ms2Table = report$MS2,
    eicTable = report$EIC,
    intStdResults = report$ISresults
  )
  
  results$intStdResults <- results$intStdResults[, c("samp", "IS", "int_h", "int_a")]
  
  results$intStdResults <- dplyr::rename(results$intStdResults, filename = samp, compound_name = IS, intensity = int_h, 
                                         area = int_a)
  class(results) <- "dbasResult"
  results
}

isEmptyReport <- function(report) {
  nrow(report$peakList) == 0
}

emptyReport <- function() {
  ntsworkflow::Report$new()
}

getField <- function(listRecords, fieldName) {
  if (!fieldAvailable(listRecords, fieldName)) {
    message("Field ", fieldName, " not found in any docs")
    return(rep(NA, length(listRecords)))
  } 
  values <- lapply(listRecords, getValueOrEmpty, field = fieldName)
  sizes <- vapply(values, length, numeric(1))
  if (all(sizes == 1)) {
    unname(unlist(values))
  } else {
    unname(values)
  }
}

fieldAvailable <- function(listRecords, fieldName) {
  any(vapply(listRecords, function(x) fieldName %in% names(x), logical(1)))
}

getValueOrEmpty <- function(record, field) {
  temp <- record[[field]]
  if (is.null(temp))
    NA else temp
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
