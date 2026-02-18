

test_that("A dummy record can be added to msrawfiles", {
  queryForTestRecord <- list(query = list(regexp = list(path = ".*RH_pos_20220603_no_peaks_test_addRecord.mzXML")))
  if (getNrow(dbComm, testIndexName, queryForTestRecord) > 0)
    deleteRow(dbComm, testIndexName, queryForTestRecord)
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "water", duration = "P1D", station = "rhein_ko_l")
  saveDir <- withr::local_tempdir()
  suppressMessages(
    addRawfiles(
      rfIndex = testIndexName, 
      templateId = templateId,
      newPaths = testFile, 
      dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
      prompt = F,
      saveDirectory = saveDir
    )  
  )
  
  importedRecord <- jsonlite::read_json(list.files(saveDir, f = T))
  expect_length(importedRecord[[1]], 79)
  recs <- getTableAsRecords(dbComm, testIndexName, queryForTestRecord)
  pathResult <- recs[[1]]$path
  expect_equal(normalizePath(testFile), pathResult)
  startResult <- recs[[1]]$start
  expect_equal(startResult, "2022-06-03")
  deleteRow(dbComm, testIndexName, queryForTestRecord)
  file.remove(list.files(saveDir, f = T))
  refreshTable(dbComm, testIndexName)
  numFound <- getNrow(dbComm, testIndexName, queryForTestRecord)
  expect_equal(numFound, 0)
})

test_that("Adding a file with malformed date results in an error", {
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_2206033_no_peaks_test_addRecord_malformedDate.mzXML")
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  saveDir <- withr::local_tempdir()
  expect_error(
    suppressMessages(
      suppressWarnings(
        idAdded <- addRawfiles(
          rfIndex = testIndexName, 
          templateId = templateIdFromMsrawfilesIndex,
          newPaths = testFile, 
          dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
          prompt = F,
          saveDirectory = saveDir
        )
      )
    )
  )
  
  dbComm <- getDbComm()
  numFound <- getNrow(dbComm, testIndexName, list(query = list(
    regexp = list(path = ".*RH_pos_2206033_no_peaks_test_addRecord_malformedDate.mzXML")
  )))
  expect_equal(numFound, 0)
})

test_that("Location information is copied from msrawfiles based on dbas_station_regex of template", {
  newFileName <- "Des_99_02_pos.mzXML"
  qTest <- list(query = list(term = list(filename = newFileName)))
  if (getNrow(dbComm, testIndexName, qTest) > 0)
    deleteRow(dbComm, testIndexName, qTest)
  saveDir <- withr::local_tempdir()
  newFilePath <- file.path(saveDir, newFileName)
  file.create(newFilePath)
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "spm", duration = "P1Y", station = "mulde_de_m")
  suppressWarnings(addRawfiles(
    rfIndex = testIndexName, 
    templateId = templateId,
    newPaths = newFilePath, 
    newStation = "filename",
    dirMeasurmentFiles = saveDir,
    prompt = F,
    saveDirectory = saveDir
  ))
  rec <- getTableAsRecords(dbComm, testIndexName, qTest, field = c("station", "loc", "river", "path"))[[1]]
  expect_equal(rec$station, "mulde_de_m")
  expect_type(rec$loc, "list")
  expect_true(grepl(newFileName, rec$path))
  deleteRow(dbComm, testIndexName, qTest)
})

test_that("Batch with an unknown filename is not processed", {
  newFileNames <- c("Des_99_02_pos.mzXML", "Foo_99_02_pos.mzXML")
  qTest <- list(query = list(terms = list(filename = newFileNames)))
  if (getNrow(dbComm, testIndexName, qTest) > 0)
    deleteRow(dbComm, testIndexName, qTest)
  saveDir <- withr::local_tempdir()
  newFilePaths <- file.path(saveDir, newFileNames)
  file.create(newFilePaths)
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "spm", duration = "P1Y", station = "mulde_de_m")
  expect_snapshot_error(suppressWarnings(addRawfiles(
    rfIndex = testIndexName, 
    templateId = templateId,
    newPaths = newFilePaths, 
    newStation = "filename",
    dirMeasurmentFiles = saveDir,
    prompt = F,
    saveDirectory = saveDir
  )))
  expect_equal(getNrow(dbComm, testIndexName, qTest), 0)
})

test_that("A station can be passed as a list", {
  newFileName <- "Foo_07_01_pos.mzXML"
  qTemplateFile <- list(query = list(term = list(filename = newFileName)))
  if (getNrow(dbComm, testIndexName, qTemplateFile) > 0)
    deleteRow(dbComm, testIndexName, qTemplateFile)
  saveDir <- withr::local_tempdir()
  newFilePath <- file.path(saveDir, newFileName)
  file.create(newFilePath)
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "spm", duration = "P1Y", station = "mulde_de_m")
  suppressWarnings(addRawfiles(
    rfIndex = testIndexName, 
    templateId = templateId,
    newPaths = newFilePath, 
    newStation = "newStationList",
    dirMeasurmentFiles = saveDir,
    prompt = F,
    saveDirectory = saveDir,
    newStationList = list(
      station = "foobar_station",
      loc = list(lat = 1.23456, lon = 1.23456),
      river = "foobar_river",
      gkz = 99999,
      km = 99999
    )
  ))
  rec <- getTableAsRecords(dbComm, testIndexName, qTemplateFile, fields = c("station", "loc", "river", "gkz", "km"))
  expect_equal(rec[[1]]$station, "foobar_station")
  expect_equal(rec[[1]]$river, "foobar_river")
  expect_equal(rec[[1]]$gkz, 99999)
  expect_equal(rec[[1]]$loc$lon, 1.23456)
  
  deleteRow(dbComm, testIndexName, qTemplateFile)
})

test_that("Batch with two different locations is correctly processed", {
  # Add dummy record for fictitious station "foobar_station", which is encoded in the filename as just "Foo"
  newFileName <- "Foo_07_01_pos.mzXML"
  qTemplateFile <- list(query = list(term = list(filename = newFileName)))
  if (getNrow(dbComm, testIndexName, qTemplateFile) > 0)
    deleteRow(dbComm, testIndexName, qTemplateFile)
  saveDir <- withr::local_tempdir()
  newFilePath <- file.path(saveDir, newFileName)
  file.create(newFilePath)
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "spm", duration = "P1Y", station = "mulde_de_m")
  rec <- getTableAsRecords(dbComm, testIndexName, list(query = list(ids = list(values = templateId))))
  rec[[1]]$station <- "foobar_station"
  rec[[1]]$loc$lon <- 1.2345
  rec[[1]]$river <- "foobar_river"
  rec[[1]]$path <- newFilePath
  rec[[1]]$start_date_regex <- "Foo_(\\d{2})_"
  appendRecords(dbComm, testIndexName, rec)
  
  # Add new batch with two different stations
  newFileNames <- c("Des_99_02_pos.mzXML", "Foo_99_02_pos.mzXML")
  qTest <- list(query = list(terms = list(filename = newFileNames)))
  if (getNrow(dbComm, testIndexName, qTest) > 0)
    deleteRow(dbComm, testIndexName, qTest)
  newFilePaths <- file.path(saveDir, newFileNames)
  file.create(newFilePaths)
  templateId <- findTemplateId(testIndexName, blank = FALSE, matrix = "spm", duration = "P1Y", station = "mulde_de_m")
  suppressWarnings(addRawfiles(
    rfIndex = testIndexName, 
    templateId = templateId,
    newPaths = newFilePaths, 
    newStation = "filename",
    dirMeasurmentFiles = saveDir,
    prompt = F,
    saveDirectory = saveDir
  ))
  
  # Test result
  recs <- getTableAsRecords(dbComm, testIndexName, qTest)
  expect_contains(map_chr(recs, \(x) x$station), "foobar_station")
  expect_contains(map_chr(recs, \(x) x$station), "mulde_de_m")
  expect_all_equal(map_chr(recs, \(x) x$start), "1999-01-01")
  expect_contains(map_dbl(recs, \(x) x$loc$lon), 1.2345)
  expect_contains(map_chr(recs, \(x) x$river), "foobar_river")
  
  # cleanup
  deleteRow(dbComm, testIndexName, qTest)
  deleteRow(dbComm, testIndexName, qTemplateFile)
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
