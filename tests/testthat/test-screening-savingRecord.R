


test_that("Simple record can be saved as json", {
  featureRecord <- getFeatureRecord()
  tempSaveDir <- withr::local_tempdir()
  
  fileNameCompressed <- saveRecord(featureRecord, tempSaveDir)
  fileName <- uncompressJson(fileNameCompressed)
  jsonText <- readLines(fileName)
  checkForStation(jsonText)
  checkForAlias(jsonText)
  
  expect_match(fileName, "unit_tests")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("An empty record is saved as an empty json", {
  emptyRecord <- getEmptyRecord()
  tempSaveDir <- withr::local_tempdir()
  fileName <- saveRecord(emptyRecord, tempSaveDir)
  
  jsonText <- readLines(fileName)
  expect_true(any(grepl("path", jsonText)))
  checkForAlias(jsonText)
  expect_match(fileName, "no-peaks")
  expect_length(jsonText, 6)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})
