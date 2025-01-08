# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

scanBatchDbas <- function(records) {
  reports <- purrr::map(records, fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  cleanedReport <- cleanReport(mergedReport, records)
  reintegratedReport <- reintegrateReport(cleanedReport)
  dbasResults <- convertToDbasResult(reintegratedReport)
  dbasResults
}


fileScanDbas <- function(msrawfileRecord, compsToProcess = NULL) {
  fileScannerDbas <- createScannerDbas(msrawfileRecord)
  fileScannerDbas <- runScanningDbas(fileScannerDbas, compsToProcess)
  fileScannerDbas
}


createScannerDbas <- function(msrawfileRecord) {
  rec <- msrawfileRecord
  scanner <- Report$new()
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
  
  scanner <- changeNameChromMethod(scanner, rec)
  scanner
}

changeNameChromMethod <- function(scannerObj, msrawfileRecord) {
  # Inconsistency in naming of chromatographic method between NTSPortal and CSL
  # Corrects the naming so that it works for CSL
  if (msrawfileRecord$chrom_method == "bfg_nts_rp1") {  # new name
    scannerObj$changeSettings("chromatography", "dx.doi.org/10.1016/j.chroma.2015.11.014")  # old name
  } else {
    scannerObj$changeSettings("chromatography", msrawfileRecord$chrom_method)
  }
  scannerObj
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
  
  falsePositives <- unique(c(
    unlist(getField(records, "dbas_fp")[!blanks][1]),
    getCompoundsBelowMinimumDetections(report, minimumDetections),
    getCompoundsNoReplicateDetections(report, replicateRegex)
  ))
  
  if (length(falsePositives) > 0) {
    for (fp in falsePositives) {
      report$deleteFP(fp)
    }
  }
  report
}

removeDuplicateDetections <- function(report) {
  report$peakList <- report$peakList[report$peakList$peak == "A", ]
  report
}

getCompoundsBelowMinimumDetections <- function(report, minimumDetections) {
  if (minimumDetections > 1) {
    timesFound <- by(report$peakList, report$peakList$comp_name, nrow)
    scarce <- which(timesFound < minimumDetections)
    names(scarce)  
  } else {
    character()
  }
}

getCompoundsNoReplicateDetections <- function(report, replicateRegex) {
  if (is.na(replicateRegex)) {
    character(0)
  } else {
    pl <- report$peakList
    pl$originalSampName <- stringr::str_replace(pl$samp, replicateRegex, "\\1")
    replicateCount <- by(pl, list(pl$comp_name, pl$originalSampName), nrow)
    replicateCount <- array2DF(replicateCount)
    colnames(replicateCount) <- c("compName", "sampName", "count")
    replicateCount <- replicateCount[!is.na(replicateCount$count), ]
    
    # Take the average number of times a compound is found in each batch,
    # so if in one replicate set it is only found once, it still doesn't count as an FP
    batchCount <- data.frame(
      compName = unique(replicateCount$compName),
      count = round(tapply(replicateCount$count, replicateCount$compName, mean))
    )
    # Hard coded: Comp found at (on average) at least twice in replicates
    batchCount[batchCount$count < 2, "compName"]
  }
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
    isResults = report$ISresults
  )
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
    unlist(values)
  } else {
    values
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








# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

