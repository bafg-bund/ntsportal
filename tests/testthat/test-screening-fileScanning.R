


test_that("A sample and blank can be scanned", {
  recordsBatch <- getRecordsSampleAndBlank()
  startTime <- Sys.time()
  results <- scanBatchDbas(recordsBatch)
  endTime <- Sys.time()
  message("Time needed to processes sample and blank: ", round(difftime(endTime, startTime, units = "secs")), " s")
  expect_true(nrow(results$peakList) > 0)
  expect_s3_class(results, "dbasResult")
})

test_that("A file with no peaks returns an empty result", {
  record <- getRecordNoPeaks()
  results <- scanBatchDbas(list(record))
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
  reports <- readRDS(test_path("fixtures", "screening-fileScanning", "reportsForMerging.RDS"))
  report <- mergeReports(reports)
  expect_s4_class(report, "Report")
})

test_that("A Report can be cleaned", {
  report <- readRDS(test_path("fixtures", "screening-fileScanning", "reportForCleaning.RDS"))
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
  records <- getRecordsSampleAndBlank()
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

test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol", {
  recordDes_07_01 <- getSingleRecordDes_07_01_pos()
  
  fileResult <- fileScanDbas(msrawfileRecord = recordDes_07_01, compsToProcess = "Bisoprolol")
  
  expect_s4_class(fileResult, "Report")
  expect_equal(fileResult$peakList$comp_name, "Bisoprolol")
})

test_that("Test file with no peaks can be processed", {
  recordNoPeak <- getRecordNoPeaks()
  with_log_threshold(
    report <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol"),
    threshold = OFF
  )
  expect_equal(nrow(report$peakList), 0)
})

test_that("If a scanning result is an error, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) stop("Test error"))
  recordNoPeak <- getRecordNoPeaks()
  resultsReport <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol")
  expect_equal(nrow(resultsReport$peakList), 0)
  withr::deferred_run()
})

test_that("If a scanning result is a try-error, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) try(stop("Test error")))
  recordNoPeak <- getRecordNoPeaks()
  resultsReport <- fileScanDbas(msrawfileRecord = recordNoPeak, compsToProcess = "Bisoprolol")
  expect_equal(nrow(resultsReport$peakList), 0)
  withr::deferred_run()
})

test_that("If a scanning result is NULL, it returns an empty report", {
  local_mocked_bindings(placeholderToSetMockingFunctions = function(...) NULL)
  recordNoPeak <- getRecordNoPeaks()
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



