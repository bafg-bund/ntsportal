# first source helper
source("tests/testthat/helper.R")
# Create new msrawfiles_unit_tests index
createNewMsrawfileUnitTestsIndex <- function() {
  dbComm <- getDbComm()
  copyTable(dbComm, tableName = "ntsp25.3_msrawfiles_unit_tests", newTableName = testIndexName, mappingType = "msrawfiles")
  changeAllDbasAliasNames(msrawfilesName = testIndexName, version = ntspVersion)
}

# deleteIndex(testIndexName)
buildMsrawfilesAllRecords <- function() {
  dbComm <- getDbComm()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  saveRDS(allRecords, test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  recordsNoClass <- lapply(allRecords, unclass)
  saveRDS(recordsNoClass, test_path("fixtures", "msrawfilesTestRecords", "allRecordsNoClass.RDS"))
  allRecordsDbas <- getTableAsRecords(
    dbComm, testIndexName, recordConstructor = newDbasMsrawfilesRecord, 
    fields = c(msrawfilesFieldsForProcessing("dbas"), msrawfileFieldsForValidation())
  )
  saveRDS(allRecordsDbas, test_path("fixtures", "msrawfilesTestRecords", "allDbasMsrawfileRecords.RDS"))
  allRecordsNts <- getTableAsRecords(
    dbComm, testIndexName, recordConstructor = newNtsMsrawfilesRecord, 
    fields = c(msrawfilesFieldsForProcessing("nts"), msrawfileFieldsForValidation())
  )
  saveRDS(allRecordsNts, test_path("fixtures", "msrawfilesTestRecords", "allNtsMsrawfileRecords.RDS"))
}

buildOneSampleDbasResult <- function() {
  records <- getOneSampleRecords("dbas")
  records <- newDbasMsrawfilesBatch(records)
  result <- scanBatch(records)
  saveRDS(result, test_path("fixtures", "screening-dbasConvertToRecord", "oneSampleDbasResult.RDS"))
}

recreateReportForCleaning <- function() {
  reports <- purrr::map(getRecordsTripicateBatch(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-dbasFileScanning", "reportForCleaning.RDS"))
}

recreateMergedReportSampleAndBlankDbas <- function() {
  reports <- purrr::map(getOneSampleRecords("dbas"), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-dbasFileScanning", "mergedReportSampleAndBlank.RDS"))
}


createResultSampleAndBlankNts <- function() {
  testBatchRecords <- getOneSampleRecords("nts")
  testNtsResults <- scanBatch(testBatchRecords)  
  saveRDS(testNtsResults, test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
}

createNtsResultBisoprololBatch <- function() {
  batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  recs <- getMsrawfilesTestRecords()
  recs <- keep(recs, \(rec) grepl(batchDirectory, rec$path))
  testNtsResults <- scanBatchNts(recs)
  saveRDS(testNtsResults, test_path("fixtures", "screening-nts", "ntsResultBisoprololBatch.RDS"))
}

createMergedReportDessauBatch <- function() {
  library(ntspQatools)
  dbComm <- getDbComm()
  recs <- getTableAsRecords(
    dbComm,
    "ntsp25.3_msrawfiles",
    list(query = list(regexp = list(path = ".*dessau.*/mud_pos/.*"))),
    newMsrawfilesRecord
  )
  filePaths <- map_chr(recs, \(x) x$path)
  newDir <- withr::local_tempdir()
  newPaths <- file.path(newDir, basename(filePaths))
  walk2(filePaths, newPaths, \(x, y) subsetMzxml(x, y, c(7,8)))
  newPaths <- normalizePath(newPaths)
  recs2 <- map2(recs, newPaths, \(x, y) {x$path <- y; x})
  recs2 <- newDbasMsrawfilesBatch(recs2)
  progBar <- cli_progress_bar("Processing batch", total = length(recs2))
  reports <- purrr::map(recs2, fileScanDbas, progBar = progBar)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  mergedReport$MS1 <- data.frame()
  mergedReport$MS2 <- data.frame()
  mergedReport$EIC <- data.frame()
  saveRDS(mergedReport, test_path("fixtures", "screening-dbasFileScanning", "mergedReportDessauBatch.RDS"))
  saveRDS(recs2, test_path("fixtures", "screening-dbasFileScanning", "recordsDessauBatch.RDS"))
}

# Create featureRecordExampleRds ####
tempSaveDir <- withr::local_tempdir()
pathToFixturesDir <- test_path("fixtures", "featureRecordExampleRds")
file.remove(list.files(pathToFixturesDir, full.names = T))
file.remove(pathToFixturesDir)
batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)
dir.create(pathToFixturesDir)
file.copy(pathRds, test_path("fixtures", "featureRecordExampleRds"))
file.remove(list.files(tempSaveDir, full.names = T))
# Test the result
test <- readRDS(list.files(pathToFixturesDir, full.names = T))


# Create duplicate feature test (4 and 5 Benzotriazole, Olmesartan) for dbasConvertToRecord
library(ntspQaTools)
fldr <- file.path("fixtures", "screening-dbasConvertToRecord")
measFilePath <- test_path(fldr, "OBF39601_20221114_Dorfbach_Oberschindmaas_neg_45benz.mzXML")
subsetMzxml("/beegfs/nts/ntsportal/msrawfiles/sachsen/neg/paket33/OBF39601_20221114_Dorfbach_Oberschindmaas_neg.mzXML",
            measFilePath, c(6.8, 7.5))
msRec <- getTableAsRecords(
  getDbComm(), 
  "ntsp25.3_msrawfiles", 
  searchBlock = list(query = list(term = list(filename = "OBF39601_20221114_Dorfbach_Oberschindmaas_neg.mzXML"))),
  recordConstructor = newDbasMsrawfilesRecord
)
msRec[[1]]$path <- normalizePath(measFilePath)
msrBatch <- newDbasMsrawfilesBatch(msRec)
saveRDS(msrBatch, test_path(fldr, "msrawfilesBatchOneSampleWithDuplicatePeaks.RDS"))
res1 <- scanBatch(msrBatch)
saveRDS(res1, test_path(fldr, "scanResultOneSampleWithDuplicatePeaks.RDS"))

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
