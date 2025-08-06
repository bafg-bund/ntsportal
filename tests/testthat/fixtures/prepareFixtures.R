# first source helper
source("tests/testthat/helper.R")
# Create new msrawfiles_unit_tests index
createNewMsrawfileUnitTestsIndex <- function() {
  dbComm <- getDbComm()
  copyTable(dbComm, tableName = "ntsp25.1_msrawfiles_unit_tests", newTableName = testIndexName, mappingType = "msrawfiles")
  changeAllDbasAliasNames(msrawfilesName = testIndexName, version = ntspVersion)
}


# deleteIndex(testIndexName)
buildMsrawfilesAllRecords <- function() {
  dbComm <- getDbComm()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  saveRDS(allRecords, test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
}

buildOneSampleDbasResult <- function() {
  records <- getOneSampleRecords()
  result <- scanBatchDbas(records)
  saveRDS(result, test_path("fixtures", "screening-convertToRecord", "oneSampleDbasResult.RDS"))
}



buildAllRecordsFlat <- function() {
  dbComm <- getDbComm()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
}

recreateReportForCleaning <- function() {
  reports <- purrr::map(getRecordsTripicateBatch(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "reportForCleaning.RDS"))
}

recreateMergedReportSampleAndBlank <- function() {
  reports <- purrr::map(getRecordsSampleAndBlank(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "mergedReportSampleAndBlank.RDS"))
}

createMergedReportTriplicateBatch <- function() {
  reports <- purrr::map(getRecordsTripicateBatch(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "mergedReportTriplicateBatch.RDS"))
}

createMergedReportDessauBatch <- function() {
  library(ntspQatools)
  dbComm <- getDbComm()
  recs <- getTableAsRecords(
    dbComm,
    "ntsp25.2_msrawfiles",
    list(query = list(regexp = list(path = ".*dessau.*/mud_pos/.*"))),
    newMsrawfilesRecord
  )
  filePaths <- map_chr(recs, \(x) x$path)
  newDir <- withr::local_tempdir()
  newPaths <- file.path(newDir, basename(filePaths))
  walk2(filePaths, newPaths, \(x, y) subsetMzxml(x, y, c(7,8)))
  newPaths <- normalizePath(newPaths)
  recs2 <- map2(recs, newPaths, \(x, y) {x$path <- y; x})
  progBar <- cli_progress_bar("Processing batch", total = length(recs2))
  reports <- purrr::map(recs2, fileScanDbas, progBar = progBar)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  mergedReport$MS1 <- data.frame()
  mergedReport$MS2 <- data.frame()
  mergedReport$EIC <- data.frame()
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "mergedReportDessauBatch.RDS"))
  saveRDS(recs2, test_path("fixtures", "screening-fileScanning", "recordsDessauBatch.RDS"))
}

# Create featureRecordExampleRds ####
tempSaveDir <- withr::local_tempdir()
batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
pathRds <- dbaScreeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)
dir.create(test_path("fixtures", "featureRecordExampleRds"))
file.copy(pathRds, test_path("fixtures", "featureRecordExampleRds"))
file.remove(list.files(tempSaveDir, full.names = T))

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
