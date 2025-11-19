
connectNtsportal()

rootDirectoryForTestMsrawfiles <- "/beegfs/nts/ntsportal/msrawfiles/unit_tests"
ntspVersion <- "25.3"

testIndexName <- glue("ntsp{ntspVersion}_msrawfiles_unit_tests")

# Since the feature record is empty, it doesn't matter which processing is used.
getEmptyFeatureRecord <- function() {
  emptyResult <- convertToDbasResult(emptyReport())
  convertToRecord(emptyResult, list(getMsrawfilesRecordNoPeaks("dbas")))
}

checkForAlias <- function(record) {
  expect_match(record$feature_table_alias, "ntsp\\d{2}\\.\\d+_feature_unit_tests")
}

getOneSampleRecords <- function(screeningType) {
  recs <- getMsrawfilesTestRecords(screeningType)
  recs <- keep(recs, \(rec) grepl("KO_06_1_pos|BL_1_pos", rec$path))
  recordsToOneBatch(recs)
}

getRecordBatchNoPeaks <- function(screeningType) {
  recordsToOneBatch(list(getMsrawfilesRecordNoPeaks(screeningType)))
}

getSingleRecordBatchDes0701pos <- function(screeningType) {
  records <- getMsrawfilesTestRecords(screeningType)
  rec <- keep(records, \(rec) grepl("Des_07_01_pos", rec$path))
  recordsToOneBatch(rec)
}


getMsrawfilesRecordNoPeaks <- function(screeningType) {
  records <- getMsrawfilesTestRecords(screeningType)
  allBatches <- splitRecordsByDir(records)
  allBatches[[grep("unit_tests/no-peaks", names(allBatches))]][[1]]
}

getMsrawfilesTestRecords <- function(screeningType) {
  switch(screeningType,
    dbas = readRDS(test_path("fixtures", "msrawfilesTestRecords", "allDbasMsrawfileRecords.RDS")),
    nts = readRDS(test_path("fixtures", "msrawfilesTestRecords", "allNtsMsrawfileRecords.RDS")),
    general = readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS")),
    no_class = readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecordsNoClass.RDS"))
  )
}

getExampleCslAsRecords <- function() {
  suppressMessages(
    records <- SpecLibRecords$new(test_path("fixtures", "CSL_olmesartan-d6.db"))
  )
}

removeExtraTestFile <- function(filename) {
  searchBlock <- list(query = list(regexp = list(filename = filename)))
  dbComm <- getDbComm()
  if (getNrow(dbComm, testIndexName, searchBlock) == 1)
    deleteRow(dbComm, testIndexName, searchBlock)
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
