
test_that("A long list of feature records are saved to one file", {
  featureRecordList <- rep(getEmptyFeatureRecord(), times = 100000)
  tempSaveDir <- withr::local_tempdir()

  fileName <- saveRecord(featureRecordList, tempSaveDir)
  
  expect_length(fileName, 1)
  expect_match(fileName, "-featureRecord-.*\\.RDS$")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Simple record can be saved as RDS", {
  featureRecordList <- getEmptyFeatureRecord()
  tempSaveDir <- withr::local_tempdir()
  
  fileName <- saveRecord(featureRecordList, tempSaveDir)
  recs <- readRDS(fileName)
  expect_contains(names(recs[[1]]), "path")
  checkForAlias(recs[[1]])
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("An empty record is saved as an empty RDS", {
  emptyRecordList <- getEmptyFeatureRecord()
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
  featureRecordList <- getEmptyFeatureRecord()
  fileName <- makeFileNameForBatch(featureRecordList)
  expect_match(fileName, "-featureRecord-\\d{6}-\\d{4}-")
  featureRecordList[[1]]$path <- "/some/other/path"
  fileName2 <- makeFileNameForBatch(featureRecordList)
  expect_true(fileName != fileName2)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
