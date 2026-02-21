
test_that("You can process a batch for nts", {
  testBatchRecords <- getOneSampleRecords("nts")
  startTime <- Sys.time()
  testNtsResults <- scanBatch(testBatchRecords)  
  endTime <- Sys.time()
  message("Time needed to processes sample and blank: ", round(difftime(endTime, startTime, units = "secs")), " s")  # 18 sec
  expect_s3_class(testNtsResults, "ntsResult")
  expect_equal(length(testNtsResults), 4)
  expect_gt(nrow(testNtsResults$alignmentTable), 10)
  expect_equal(nrow(testNtsResults$annotationTable), 0)
  expect_gte(testNtsResults$sampleList[1, "normalizePeakId"], 1)
  expect_contains(colnames(testNtsResults$sampleList), c("intStdName", "featureAliasName", "eicExtractionWidth"))
  expect_contains(colnames(testNtsResults$peakList), c("rt", "rightEndRt", "peakId", "sampleId", "area", "baseline"))
})

test_that("You can create a peakPickingResult from a small file", {
  testRec <- getSingleRecordBatchDes0701pos("nts")
  testResult <- getPeakPickingResult(testRec[[1]])
  expect_lt(object.size(testResult), 2e5)
  expect_equal(ncol(testResult$finishedPeakList), 35)
  expect_gt(nrow(testResult$finishedPeakList), 30)
})

test_that("A peakPickingResult from an empty file is empty", {
  testRec <- getMsrawfilesRecordNoPeaks("nts")
  testResult <- getPeakPickingResult(testRec)
  expect_equal(nrow(testResult$finishedPeakList), 0)
})

