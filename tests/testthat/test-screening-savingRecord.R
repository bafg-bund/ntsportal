


test_that("Simple record can be saved as json", {
  simpleRecord <- getSimpleRecord()
  tempSaveDir <- withr::local_tempdir()
  
  fileNameCompressed <- saveRecord(simpleRecord, tempSaveDir)
  fileName <- uncompressJson(fileNameCompressed)
  jsonText <- readLines(fileName)
  expect_true(any(grepl("station", jsonText)))
  expect_match(fileName, "meas_files")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("An empty record is saved as an empty json", {
  emptyRecord <- getEmptyRecord()
  tempSaveDir <- withr::local_tempdir()
  fileName <- saveRecord(emptyRecord, tempSaveDir)
  
  jsonText <- readLines(fileName)
  expect_true(any(grepl("path", jsonText)))
  expect_true(any(grepl("dbas_alias_name", jsonText)))
  expect_match(fileName, "no-peaks")
  expect_length(jsonText, 6)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})
