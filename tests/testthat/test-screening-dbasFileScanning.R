


test_that("A sample and blank can be scanned", {
  recordsBatch <- getOneSampleRecords("dbas")
  startTime <- Sys.time()
  results <- scanBatch(recordsBatch)
  endTime <- Sys.time()
  message("Time needed to processes sample and blank: ", round(difftime(endTime, startTime, units = "secs")), " s")
  expect_true(nrow(results$peakList) > 0)
  expect_no_match(results$reintegrationResults$samp, "/")
  expect_s3_class(results, "dbasResult")
})

test_that("A file with no peaks returns an empty result", {
  recordBatch <- getRecordBatchNoPeaks("dbas")
  results <- scanBatch(recordBatch)
  expect_equal(nrow(results$peakList), 0)
  expect_s3_class(results, "dbasResult")
})

test_that("An empty report is removed from a list of reports", {
  recordsBatch <- getOneSampleRecords("dbas")
  report <- getMergedReportSampleAndBlank()
  reportWithEmpty <- list(report, emptyReport())
  dataBoxTest <- removeEmptyReports(list(reports = reportWithEmpty, msrawfilesBatch = recordsBatch))
  expect_equal(length(dataBoxTest$reports), 1)
  expect_equal(list(report), dataBoxTest$reports)
})

test_that("Reports can be merged", {
  testReports <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "reportsForMerging.RDS"))
  filesPreMerge <- sapply(testReports, \(x) x$rawFiles)
  reportMerged <- mergeReports(testReports)
  filesPostMerge <- reportMerged$rawFiles
  expect_equal(filesPreMerge, filesPostMerge)
  expect_s4_class(reportMerged, "Report")
})

test_that("A Report can be cleaned", {
  reportPreclean <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "reportForCleaning.RDS"))
  plPreClean <- reportPreclean$peakList
  irPreClean <- reportPreclean$integRes
  numberDetections <- table(plPreClean$comp_name)
  sporadicDetections <- names(which(numberDetections < 2))
  dataBox <- cleanReport(list(reports = reportPreclean, msrawfilesBatch = getRecordsTripicateBatch()))
  plPostClean <- dataBox$reports$peakList
  irPostClean <- dataBox$reports$integRes
  compsPostPl <- unique(plPostClean$comp_name)
  compsPostIr <- unique(irPostClean$comp_name)
  expect_disjoint(compsPostPl, sporadicDetections)
  expect_disjoint(compsPostIr, sporadicDetections)
  expect_gt(nrow(plPreClean), nrow(plPostClean))
  expect_gt(nrow(irPreClean), nrow(irPostClean))
  expect_s4_class(dataBox$reports, "Report")
})

test_that("A Report can be reintegrated", {
  report <- getMergedReportSampleAndBlank()
  originalReport <- report$copy()
  reintegratedReport <- reintegrateReport(report)
  expect_gt(nrow(reintegratedReport$integRes), nrow(originalReport$integRes))
})

test_that("A failed reintegration returns the original report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) stop("Test error"))
  report <- getMergedReportSampleAndBlank()
  originalReport <- report$copy()
  reintegratedReport <- reintegrateReport(report)
  expect_equal(originalReport, reintegratedReport)
  withr::deferred_run()
})

test_that("Blank correction works for sample and blank", {
  boxTest <- list(reports = getMergedReportSampleAndBlank(), msrawfilesBatch = getOneSampleRecords("dbas"))
  peaksBeforeCorrection <- nrow(boxTest$reports$peakList)
  allComps <- unique(boxTest$reports$peakList$comp_name)
  compsSamples <- unique(getPeakListNonBlanks(boxTest)$comp_name)
  compsBlanks <- setdiff(allComps, compsSamples)
  boxTest <- blankCorrectionDbas(boxTest)
  peaksAfterCorrection <- nrow(boxTest$reports$peakList)
  plComps <- unique(boxTest$reports$peakList$comp_name)
  irComps <- unique(boxTest$reports$integRes$comp_name)
  expect_true(all(plComps %in% compsSamples))
  expect_true(all(irComps %in% compsSamples))
  expect_false(any(compsBlanks %in% plComps))
  expect_false(any(compsBlanks %in% irComps))
  expect_lt(peaksAfterCorrection, peaksBeforeCorrection)
})

test_that("False positives are removed", {
  testData <- getReportReplicateBatch()
  originalComps <- unique(testData$report$peakList$comp_name)
  report <- removeFalsePositives(list(reports = testData$report, msrawfilesBatch = testData$records))
  newComps <- unique(report$peakList$comp_name)
  expect_lt(length(newComps), length(originalComps))
  expect_true(is.element("Propranolol", originalComps))
  expect_false(is.element("Propranolol", newComps))
}) 

test_that("Replicate detections are correctly computed", {
  reintReport <- getReintegratedReportDessauBatch()
  dataBox <- cleanReport(list(reports = getReintegratedReportDessauBatch(), msrawfilesBatch = getRecordsDessauBatch()))
  deletedComps <- setdiff(reintReport$peakList$comp_name, dataBox$reports$peakList$comp_name)
  expect_contains(deletedComps, "Olmesartan-d6")
  expect_contains(reintReport$peakList$comp_name, "Methyltriphenylphosphonium")
  expect_contains(dataBox$reports$peakList$comp_name, "Methyltriphenylphosphonium")
})

test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol", {
  batchDes0701 <- getSingleRecordBatchDes0701pos("dbas")
  
  fileResult <- fileScanDbas(msrawfileRecord = batchDes0701[[1]], compsToProcess = "Bisoprolol")
  
  expect_s4_class(fileResult, "Report")
  expect_equal(fileResult$peakList$comp_name, "Bisoprolol")
})

test_that("Test file with no peaks can be processed", {
  recordNoPeak <- getMsrawfilesRecordNoPeaks("dbas")
  with_log_threshold(
    report <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol"),
    threshold = OFF
  )
  expect_equal(nrow(report$peakList), 0)
})

test_that("If process_all fails, it returns the original, non-processed and empty report", {
  recordNoPeak <- getMsrawfilesRecordNoPeaks("dbas")
  testScanner <- createScannerDbas(recordNoPeak)
  testScanner$settings$db_path <- "foo"
  resultsReport <- runScanningDbas(testScanner, "bar")
  expect_length(resultsReport$rawData, 0)
  expect_equal(nrow(resultsReport$peakList), 0)
})

test_that("Getting non-existent field returns an NA vector", {
  records <- getRecordsTripicateBatch()
  values <- getField(records, "foobar")
  expect_true(all(is.na(values)))
})

test_that("When unitary field missing in one record the ruturn value has one NA", {
  records <- getRecordsTripicateBatch()
  records[[2]]$path <- NULL
  values <- getField(records, "path")
  expect_true(is.na(values[[2]]))
})

test_that("When non unitary field missing in one record the ruturn value is a list with one NA", {
  records <- getRecordsTripicateBatch()
  records[[2]]$dbas_fp <- NULL
  values <- getField(records, "dbas_fp")
  expect_true(is.na(values[[2]]))
})

test_that("Removing compounds with no replicate detections works for level 1 and level 2 annotations", {
  testReportBim <- ntsworkflow::loadReport(F, test_path("fixtures", "screening-dbasFileScanning", "reintegratedReportBimmen.RDS"))
  testRecordsBim <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "batchBimmen.RDS"))
  # check that "3-Phenylpyridine" in B14
  before <- testReportBim$integRes
  compsFoundBefore <- subset(before, samp == "B14_pos1.mzXML", comp_name, drop = T)
  expect_contains(compsFoundBefore, "Benzyl-triethylammonium")
  expect_contains(compsFoundBefore, "3-Phenylpyridine")
  cleanedReportBox <- cleanReport(list(reports = testReportBim, msrawfilesBatch = testRecordsBim))
  dbasResult <- convertToDbasResult(cleanedReportBox$reports)
  after <- dbasResult$reintegrationResults
  compsFoundAfter <- subset(after, samp == "B14_pos1.mzXML", comp_name, drop = T)
  expect_contains(compsFoundAfter, "Benzyl-triethylammonium")
  expect_disjoint("3-Phenylpyridine", compsFoundAfter)
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

