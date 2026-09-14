

# Script to create fixtures and other one-time setup for unit tests
# Can only be run after you have created the msrawfiles for the current version (e.g. ntsp26.1_msrawfiles)

# Bash commands to start script ####
# nohup Rscript tests/testthat/fixtures/prepareFixtures.R &> tests/testthat/fixtures/prepareFixtures.log &
# tail -f tests/testthat/fixtures/prepareFixtures.log

# Setup ####
devtools::load_all()
library(ntspQaTools)

currentMsr <- glue("ntsp{ntspVersion}_msrawfiles")

log_info("Start")

# msrawfiles allRecords ####
log_info("Section: msrawfiles allRecords")
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

# oneSampleDbasResult ####
log_info("Section: oneSampleDbasResult")
records <- getOneSampleRecords("dbas")
records <- newDbasMsrawfilesBatch(records)
result <- scanBatch(records)
saveRDS(result, test_path("fixtures", "screening-dbasConvertToRecord", "oneSampleDbasResult.RDS"))

# reportForCleaning ####
log_info("Section: reportForCleaning")
reports <- purrr::map(getRecordsTripicateBatch(), fileScanDbas)
dataBoxCleaning <- removeEmptyReports(list(reports = reports, msrawfilesBatch = getRecordsTripicateBatch()))
dataBoxCleaning$reports <- mergeReports(dataBoxCleaning$reports)
dataBoxCleaning$reports <- reintegrateReport(dataBoxCleaning$reports)
saveRDS(dataBoxCleaning$reports, test_path("fixtures", "screening-dbasFileScanning", "reportForCleaning.RDS"))

# Report for testing false positive removal blankCorrectedReportTriplicate ####
log_info("Section: Report for testing false positive removal blankCorrectedReportTriplicate")
records <- getMsrawfilesTestRecords("dbas")
allBatches <- splitRecordsByDir(records)
recs <- allBatches[[grep("unit_tests/olmesartan-d6-bisoprolol", names(allBatches))]]
recsBatch <- newDbasMsrawfilesBatch(recs)
reports <- map(recsBatch, fileScanDbas)
dataBoxFP <- removeEmptyReports(list(reports = reports, msrawfilesBatch = recsBatch))
dataBoxFP$reports <- mergeReports(dataBoxFP$reports)
dataBoxFP$reports <- reintegrateReport(dataBoxFP$reports)
saveRDS(dataBoxFP$reports, test_path("fixtures", "screening-dbasFileScanning", "blankCorrectedReportTriplicate.RDS"))

# mergedReportSampleAndBlank ####
log_info("Section: mergedReportSampleAndBlank")
reports <- purrr::map(getOneSampleRecords("dbas"), fileScanDbas)
dataBoxOneSamp <- removeEmptyReports(list(reports = reports, msrawfilesBatch = getOneSampleRecords("dbas")))
dataBoxOneSamp$reports <- mergeReports(dataBoxOneSamp$reports)
saveRDS(dataBoxOneSamp$reports, test_path("fixtures", "screening-dbasFileScanning", "mergedReportSampleAndBlank.RDS"))

# ntsResultSampleAndBlank ####
log_info("Section: ntsResultSampleAndBlank")
testBatchRecords <- getOneSampleRecords("nts")
testNtsResults <- scanBatch(testBatchRecords)  
saveRDS(testNtsResults, test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))

# ntsResultBisoprololBatch ####
log_info("Section: ntsResultBisoprololBatch")
batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
recs <- getMsrawfilesTestRecords(screeningType = "nts")
recs <- keep(recs, \(rec) grepl(batchDirectory, rec$path))
recs <- newNtsMsrawfilesBatch(recs)
testNtsResults <- scanBatch(recs)
saveRDS(testNtsResults, test_path("fixtures", "screening-nts", "ntsResultBisoprololBatch.RDS"))

# reintegratedReportDessauBatch and recordsDessauBatch ####
log_info("Section: reintegratedReportDessauBatch and recordsDessauBatch")
recs <- getTableAsRecords(
  dbComm,
  currentMsr,
  list(query = list(regexp = list(path = ".*mulde_dessau_m/spm/p1y/2005_2013/pos.*"))),
  recordConstructor = newDbasMsrawfilesRecord
)
filePaths <- map_chr(recs, \(x) x$path)
newDir <- withr::local_tempdir()
newPaths <- file.path(newDir, basename(filePaths))
walk2(filePaths, newPaths, \(x, y) subsetMzxml(x, y, c(7,8)))
newPaths <- normalizePath(newPaths)
recs2 <- map2(recs, newPaths, \(x, y) {x$path <- y; newDbasMsrawfilesRecord(x)})
recs2 <- newDbasMsrawfilesBatch(recs2)
progBar <- cli_progress_bar("Processing batch", total = length(recs2))
reports <- purrr::map(recs2, fileScanDbas, progBar = progBar)
dataBoxDessau <- removeEmptyReports(list(reports = reports, msrawfilesBatch = recs2))
dataBoxDessau$reports <- mergeReports(dataBoxDessau$reports)
dataBoxDessau$reports <- reintegrateReport(dataBoxDessau$reports)
dataBoxDessau$reports$MS1 <- data.frame()
dataBoxDessau$reports$MS2 <- data.frame()
dataBoxDessau$reports$EIC <- data.frame()
saveRDS(dataBoxDessau$reports, test_path("fixtures", "screening-dbasFileScanning", "reintegratedReportDessauBatch.RDS"))
saveRDS(recs2, test_path("fixtures", "screening-dbasFileScanning", "recordsDessauBatch.RDS"))

# Create featureRecordExampleRds ####
log_info("Section: Create featureRecordExampleRds")
tempSaveDir <- withr::local_tempdir()
pathToFixturesDir <- test_path("fixtures", "featureRecordExampleRds")
file.remove(list.files(pathToFixturesDir, full.names = T))
file.remove(pathToFixturesDir)
batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)
dir.create(pathToFixturesDir)
file.copy(pathRds, pathToFixturesDir)
file.remove(list.files(tempSaveDir, full.names = T))
# Test the result
# test <- readRDS(list.files(pathToFixturesDir, full.names = T))

# Create ntsFeatureRecordExampleRds
log_info("Section: Create ntsFeatureRecordExampleRds")
tempSaveDir <- withr::local_tempdir()
pathToFixturesDir <- test_path("fixtures", "ntsFeatureRecordExampleRds")
file.remove(list.files(pathToFixturesDir, full.names = T))
file.remove(pathToFixturesDir)
batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir, screeningType = "nts")
dir.create(pathToFixturesDir)
file.copy(pathRds, pathToFixturesDir)
file.remove(list.files(tempSaveDir, full.names = T))


# Duplicate feature test (4 and 5 Benzotriazole, Olmesartan) for dbasConvertToRecord ####
log_info("Section: Duplicate feature test (4 and 5 Benzotriazole, Olmesartan) for dbasConvertToRecord")
fldr <- file.path("fixtures", "screening-dbasConvertToRecord")
measFilePath <- test_path(fldr, "OBF39601_20221114_Dorfbach_Oberschindmaas_neg_45benz.mzXML")
subsetMzxml("/beegfs/nts/ntsportal/msrawfiles/sachsen/neg/paket33/OBF39601_20221114_Dorfbach_Oberschindmaas_neg.mzXML",
            measFilePath, c(6.8, 7.5))
msRec <- getTableAsRecords(
  getDbComm(), 
  currentMsr, 
  searchBlock = list(query = list(term = list(filename = "OBF39601_20221114_Dorfbach_Oberschindmaas_neg.mzXML"))),
  recordConstructor = newDbasMsrawfilesRecord
)
msRec[[1]]$path <- normalizePath(measFilePath)
msrBatch <- newDbasMsrawfilesBatch(msRec)
saveRDS(msrBatch, test_path(fldr, "msrawfilesBatchOneSampleWithDuplicatePeaks.RDS"))
res1 <- scanBatch(msrBatch)
saveRDS(res1, test_path(fldr, "scanResultOneSampleWithDuplicatePeaks.RDS"))

# Example data from Bimmen testing replicate injection filter reintegratedReportBimmen ####
log_info("Section: Example data from Bimmen testing replicate injection filter reintegratedReportBimmen")

recs <- getTableAsRecords(
  getDbComm(), tableName = currentMsr, 
  searchBlock = 
    list(
      query = list(
        bool = list(
          must = list(
            list(
              term = list(
                station = "rhein_bi_l"
              )
            ),
            list(
              term = list(
                duration = "P1Y"
              )
            ),
            list(
              term = list(
                pol = "pos"
              )
            ),
            list(
              terms = list(
                start = list("2014-01-01", "2021-01-01")
              )
            )
          )
        )
      )
    ), 
    recordConstructor = newDbasMsrawfilesRecord
)
nbatch <- newDbasMsrawfilesBatch(recs)
saveRDS(nbatch, "tests/testthat/fixtures/screening-dbasFileScanning/batchBimmen.RDS")
reports <- purrr::map(nbatch, fileScanDbas, compsToProcess = c("3-Phenylpyridine", "Benzyl-triethylammonium"))
reports <- removeEmptyReports(reports)
mergedReport <- mergeReports(reports)
reintegratedReport <- reintegrateReport(mergedReport)
saveRDS(reintegratedReport, "tests/testthat/fixtures/screening-dbasFileScanning/reintegratedReportBimmen.RDS")

# Test record for blank correction ####
tempSaveDir <- withr::local_tempdir()
pathToFixturesDir <- test_path("fixtures", "screening-dbasFileScanning")
bt <- getSelectedMsrawfileBatches(currentMsr, "/beegfs/nts/ntsportal/msrawfiles/saale_wettin_m/spm/p1y/2024_2033/pos", "dbas")
reports <- map(bt[[1]], \(rec) fileScanDbas(rec, compsToProcess = c("Bethanidine", "Sitagliptin", "Bezafibrate-d6")))
reports <- removeEmptyReports(reports)
mergedReport <- mergeReports(reports)
reintegratedReport <- reintegrateReport(mergedReport)
saveRDS(reintegratedReport, file.path(pathToFixturesDir, "test2.RDS"))

log_info("End")

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
