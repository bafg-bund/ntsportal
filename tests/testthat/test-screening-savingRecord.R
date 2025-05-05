
test_that("A long list of feature records are saved to multiple files", {
  featureRecordList <- rep(getFeatureRecord(), times = 1000)
  tempSaveDir <- withr::local_tempdir()

  fileNames <- saveRecord(featureRecordList, tempSaveDir, maxSizeGb = 0.2)
  
  expect_length(fileNames, 2)
  expect_match(fileNames[2], "part-b")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Simple record can be saved as json", {
  Sys.getenv()
  featureRecordList <- getFeatureRecord()
  tempSaveDir <- withr::local_tempdir()
  
  fileNameCompressed <- saveRecord(featureRecordList, tempSaveDir)
  fileName <- uncompressJson(fileNameCompressed)
  jsonText <- readLines(fileName)
  checkForStation(jsonText)
  checkForAlias(jsonText)
  
  expect_match(fileName, "part-a")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("An empty record is saved as an empty json", {
  emptyRecordList <- getEmptyRecord()
  tempSaveDir <- withr::local_tempdir()
  fileName <- saveRecord(emptyRecordList, tempSaveDir)
  
  jsonText <- readLines(fileName)
  expect_true(any(grepl("path", jsonText)))
  checkForAlias(jsonText)
  expect_length(jsonText, 6)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("A filename can be correctly generated", {
  featureRecordList <- getFeatureRecord()
  attr(featureRecordList, "part") <- "q"
  fileName <- makeFileNameForBatch(featureRecordList)
  expect_match(fileName, "part-q")
  
  featureRecordList[[1]]$path <- "/some/other/path"
  fileName2 <- makeFileNameForBatch(featureRecordList)
  expect_true(fileName != fileName2)
})

test_that("Compressing a file that already exists results in an error", {
  tfile <- withr::local_tempfile()
  cat("test", file = tfile)
  newPath <- compressJson(tfile)
  expect_true(file.exists(newPath))
  expect_error(compressJson(tfile))
  file.remove(newPath)
})
