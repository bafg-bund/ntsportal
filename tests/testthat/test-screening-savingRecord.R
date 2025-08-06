
test_that("A long list of feature records are saved to multiple files", {
  featureRecordList <- rep(getFeatureRecord(), times = 1000)
  tempSaveDir <- withr::local_tempdir()

  fileNames <- saveRecord(featureRecordList, tempSaveDir, maxSizeGb = 0.1)
  
  expect_length(fileNames, 2)
  expect_match(fileNames[2], "part-b")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Simple record can be saved as RDS", {
  featureRecordList <- getFeatureRecord()
  tempSaveDir <- withr::local_tempdir()
  
  fileName <- saveRecord(featureRecordList, tempSaveDir)
  recs <- readRDS(fileName)
  expect_contains(names(recs[[1]]), "path")
  checkForAlias(recs[[1]])
  
  expect_match(fileName, "part-a")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("An empty record is saved as an empty RDS", {
  emptyRecordList <- getEmptyRecord()
  tempSaveDir <- withr::local_tempdir()
  fileName <- saveRecord(emptyRecordList, tempSaveDir)
  
  records <- readRDS(fileName)
  expect_contains(names(records[[1]]), "path")
  checkForAlias(records[[1]])
  expect_length(records, 1)
  expect_length(records[[1]], 2)
  
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

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
