#' Extract features from measurement files with an algorithm
#' @export
scanBatch <- function(msrawfilesBatch, ...) {
  UseMethod("scanBatch")
}

#' Scan measurement files for known compounds
#' @description The spectral library is used to scan mzXML measurment files for compounds using the DBAS algorithm.
#' The parameters for DBAS and file locations (measurement files and spectral library) are stored the `dbasMsrawfilesBatch`
#' object.
#' @param msrawfilesBatch object inheriting `msrawfilesBatch` (`list` of `msrawfilesRecord`s)
#' @param compsToProcess Character vector of compounds names to include in the scanning (default is all compounds in the spectral library)
#' @param showProgress Logical, show progress bar? (for interacte use, default false)
#' @returns a `dbasResult` (`list` of 7 tables) including `peakList`, `reintegrationResults` etc.
#' @rdname scanBatch
#' @examples
#' \dontrun{
#' dbComm <- getDbComm()
#' recs <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.2_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(filename = "Des_19_.._pos.mzXML"))),
#'   newDbasMsrawfilesRecord
#' )
#' recsBlanks <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.2_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(path = ".*mud_pos/BW.*"))), 
#'   newDbasMsrawfilesRecord
#' )
#' newBatch <- newDbasMsrawfilesBatch(c(recs, recsBlanks))
#' res <- scanBatch(newBatch, "Methyltriphenylphosphonium")
#' }
#' @export
scanBatch.dbasMsrawfilesBatch <- function(msrawfilesBatch, compsToProcess = NULL, showProgress = FALSE) {
  progBar <- ifelse(showProgress, cli_progress_bar("Processing batch", total = length(msrawfilesBatch)), "no-progress")
  dataBox <- list(msrawfilesBatch = msrawfilesBatch)
  dataBox$reports <- map(msrawfilesBatch, \(rec) fileScanDbas(rec, compsToProcess = compsToProcess, progBar = progBar))
  dataBox <- removeEmptyReports(dataBox)
  dataBox$reports <- mergeReports(dataBox$reports)
  dataBox$reports <- reintegrateReport(dataBox$reports)
  dataBox <- cleanReport(dataBox)
  dbasResults <- convertToDbasResult(dataBox$reports)
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
  scanner$addDB(F, rec$spectral_library_path)
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
  scanner$changeSettings("instr", unlist(rec$csl_instruments_allowed))
  scanner$changeSettings("pol", rec$pol)
  scanner$changeSettings("numcores", 1)
  scanner$changeSettings("chromatography", rec$chrom_method)
  scanner
}

runScanningDbas <- function(scannerObj, compsToProcess) {
  backupScanner <- scannerObj$copy()
  processAllError <- FALSE
  tryCatch({
    suppressMessages(scannerObj$process_all(comp_names = compsToProcess))
    warnNoPeaks(scannerObj)
  }, 
  error = function(cnd) {
    processAllError <<- TRUE
    log_error("Processing error in file: {scannerObj$rawFiles},
                  error message: {conditionMessage(cnd)}")
  },
  finally = {
    if (inherits(scannerObj, "Report"))
      scannerObj$clearData()
  })
  if (processAllError)
    backupScanner else scannerObj
}

warnNoPeaks <- function(report) {
  if (nrow(report$peakList) == 0)
    log_warn("No peaks found in file: {report$rawFiles}")
}

placeholderToSetMockingFunctions <- function(x) {
  x
}

removeEmptyReports <- function(dataBox) {
  stopifnot(length(dataBox$reports) == length(dataBox$msrawfilesBatch))
  emptyReps <- map_lgl(dataBox$reports, isEmptyReport)
  if (all(emptyReps))
    return(list(reports = dataBox$reports[1], msrawfilesBatch = dataBox$msrawfilesBatch[1]))
  list(reports = dataBox$reports[!emptyReps], msrawfilesBatch = dataBox$msrawfilesBatch[!emptyReps])
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

cleanReport <- function(dataBox) {
  stopifnot(length(dataBox$reports$rawFiles) == length(dataBox$msrawfilesBatch))
  if (!isEmptyReport(dataBox$reports)) 
    dataBox <- removeFalsePositives(dataBox)
  if (!isEmptyReport(dataBox$reports)) 
    dataBox <- removeDuplicateDetections(dataBox)
  if (!isEmptyReport(dataBox$reports)) 
    dataBox <- blankCorrectionDbas(dataBox)
  dataBox
}

removeFalsePositives <- function(dataBox) {
  falsePositives <- bind_rows(
    getFalsePositivesFromRecord(dataBox),
    getCompoundsBelowMinimumDetections(dataBox),
    getCompoundsNoReplicateDetections(dataBox)
  ) |> distinct() |> tidyr::nest(fileIndices = fileIndex, .by = "compName")
  
  if (nrow(falsePositives) > 0) { 
    for (i in 1:nrow(falsePositives)) {
      dataBox$reports$deleteFP(
        pluck(falsePositives, "compName", i),
        pluck(falsePositives, "fileIndices", i, 1)
      )
    }
  }
  dataBox
}

getFalsePositivesFromRecord <- function(dataBox) {
  files <- basename(dataBox$reports$rawFiles)  # file index is given by report$rawFiles, index needed for Report$deleteFP
  fpList <- setNames(getField(dataBox$msrawfilesBatch, "dbas_fp", quiet = TRUE), basename(getField(dataBox$msrawfilesBatch, "path")))
  if (!all(is.na(fpList))) {
    fpList <- fpList[!is.na(fpList)]
    fpRows <- map(seq_along(files), function(ind) tibble(fileIndex = ind, compName = fpList[[files[ind]]]))
    bind_rows(fpRows)
  } else {
    return(emptyFalsePostivesTibble())
  }
}

getCompoundsBelowMinimumDetections <- function(dataBox) {
  # These fields are uniform in the records so just take the first non blank
  blanks <- getField(dataBox$msrawfilesBatch, "blank")
  if (all(blanks)) {
    return(emptyFalsePostivesTibble())
  } else {
    minimumDetections <- getField(dataBox$msrawfilesBatch, "dbas_minimum_detections")[!blanks][1]
  }
  peakListNonBlanks <- getPeakListNonBlanks(dataBox)
  timesFound <- by(peakListNonBlanks, peakListNonBlanks$comp_name, nrow)
  enoughDetections <- names(which(timesFound >= minimumDetections))
  allComps <- unique(dataBox$reports$peakList$comp_name)
  scarce <- allComps[!is.element(allComps, enoughDetections)]
  if (length(scarce) == 0)
    return(emptyFalsePostivesTibble())
  tidyr::expand_grid(compName = scarce, fileIndex = seq_along(dataBox$reports$rawFiles)) 
}

getPeakListNonBlanks <- function(dataBox) {
  blanks <- getField(dataBox$msrawfilesBatch, "blank")
  fileNamesNonBlanks <- basename(getField(dataBox$msrawfilesBatch, "path"))[!blanks]
  dataBox$reports$peakList[dataBox$reports$peakList$samp %in% fileNamesNonBlanks, ]
}

getCompoundsNoReplicateDetections <- function(dataBox) {
  blanks <- getField(dataBox$msrawfilesBatch, "blank")
  repGroups <- getField(dataBox$msrawfilesBatch, "replicate_group_name", quiet = TRUE)[!blanks]
  if (all(is.na(repGroups))) 
    return(emptyFalsePostivesTibble())
  numberReps <- table(repGroups)
  if (!all(numberReps >= 2)) {
    batchName <- dirname(getField(dataBox$msrawfilesBatch, "path")[1])
    log_warn("replicate_group_name available but batch {batchName} does not have at least 2 samples in each replicate group")
    return(emptyFalsePostivesTibble())
  }
    
  replicateGroup <- tibble(
    samp = basename(getField(dataBox$msrawfilesBatch, "path", quiet = TRUE)[!blanks]),
    originalSampName = repGroups
  )
  ir <- merge(dataBox$reports$integRes, replicateGroup, by = "samp", all.x = T)
  replicateCount <- by(ir, list(ir$comp_name, ir$originalSampName), nrow) |> array2DF()
  colnames(replicateCount) <- c("compName", "originalSampName", "count")
  replicateCount <- replicateCount[!is.na(replicateCount$count), ]
  if (nrow(replicateCount) == 0) 
    return(emptyFalsePostivesTibble())
  # Regardless of the number of replicates, if compound is found at least twice, it's not added to FP list
  markAsFp <- replicateCount[replicateCount$count < 2, c("compName", "originalSampName")]
  if (nrow(markAsFp) == 0) 
    return(emptyFalsePostivesTibble())
  list_rbind(map2(
    markAsFp$compName, 
    markAsFp$originalSampName, 
    function(compName, originalSampName) 
      getFileIndices(compName, originalSampName, ir, dataBox$reports$rawFiles)
  ))
}

getFileIndices <- function(compName, originalSampName, pl, reportRawFiles) {
  sampNames <- pl[pl$originalSampName == originalSampName, "samp"] |> unique()
  tibble(
    compName = compName,
    fileIndex = which(basename(reportRawFiles) %in% sampNames)
  )
}

blankCorrectionDbas <- function(dataBox) {
  blanks <- getField(dataBox$msrawfilesBatch, "blank")
  if (all(!blanks))
    return(dataBox)
  dataBox$reports$deleteBackground(which(!blanks), which(blanks))
  dataBox$reports$remRawFiles(which(blanks))
  dataBox
}

removeDuplicateDetections <- function(dataBox) {
  dataBox$reports$peakList <- dataBox$reports$peakList[dataBox$reports$peakList$peak == "A", ]
  dataBox
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

fieldAvailable <- function(listRecords, fieldName) {
  any(vapply(listRecords, function(x) fieldName %in% names(x), logical(1)))
}

getValueOrEmpty <- function(record, field) {
  temp <- record[[field]]
  if (is.null(temp))
    NA else temp
}

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
