


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
  report <- getMergedReportSampleAndBlank()
  reportWithEmpty <- list(report, emptyReport())
  report2 <- removeEmptyReports(reportWithEmpty)
  expect_equal(length(report2), 1)
  expect_equal(list(report), report2)
})

test_that("Reports can be merged", {
  reports <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "reportsForMerging.RDS"))
  report <- mergeReports(reports)
  expect_s4_class(report, "Report")
})

test_that("A Report can be cleaned", {
  report <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "reportForCleaning.RDS"))
  report <- cleanReport(report, getRecordsTripicateBatch())
  expect_s4_class(report, "Report")
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
  report <- getMergedReportSampleAndBlank()
  records <- getOneSampleRecords("dbas")
  peaksBeforeCorrection <- nrow(report$peakList)
  report <- blankCorrectionDbas(report, records)
  peaksAfterCorrection <- nrow(report$peakList)
  expect_lt(peaksAfterCorrection, peaksBeforeCorrection)
})

test_that("False positives are removed", {
  testData <- getReportReplicateBatch()
  originalComps <- unique(testData$report$peakList$comp_name)
  report <- removeFalsePositives(testData$report, testData$records)
  newComps <- unique(report$peakList$comp_name)
  expect_lt(length(newComps), length(originalComps))
  expect_true(is.element("Propranolol", originalComps))
  expect_false(is.element("Propranolol", newComps))
}) 

test_that("Replicate detections are correctly computed", {
  mergedReport <- getMergedReportDessauBatch()
  cleanedReport <- cleanReport(getMergedReportDessauBatch(), getRecordsDessauBatch())
  deletedComps <- setdiff(mergedReport$peakList$comp_name, cleanedReport$peakList$comp_name)
  expect_contains(deletedComps, "Olmesartan-d6")
  expect_contains(mergedReport$peakList$comp_name, "Methyltriphenylphosphonium")
  expect_contains(cleanedReport$peakList$comp_name, "Methyltriphenylphosphonium")
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

test_that("If a scanning result is an error, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) stop("Test error"))
  recordNoPeak <- getMsrawfilesRecordNoPeaks("dbas")
  resultsReport <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol")
  expect_equal(nrow(resultsReport$peakList), 0)
  withr::deferred_run()
})

test_that("If a scanning result is a try-error, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) try(stop("Test error")))
  recordNoPeak <- getMsrawfilesRecordNoPeaks("dbas")
  resultsReport <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol")
  expect_equal(nrow(resultsReport$peakList), 0)
  withr::deferred_run()
})

test_that("If a scanning result is NULL, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) NULL)
  recordNoPeak <- getMsrawfilesRecordNoPeaks("dbas")
  resultsReport <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol")
  expect_equal(nrow(resultsReport$peakList), 0)
  withr::deferred_run()
})

test_that("Getting non existent field returns an NA vector", {
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
  testRecordsBim <- test_path("fixtures", "screening-dbasFileScanning", "testRecordBimmen.RDS")
  cleanedReport <- cleanReport(testReportBim, testRecordsBim)
  dbasResult <- convertToDbasResult(cleanedReport)
  reint <- dbasResult$reintegrationResults
  expect_contains(reint$comp_name, "Benzyl-triethylammonium")
  expect_false(is.element(reint$comp_name, "3-Phenylpyridine"))
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

