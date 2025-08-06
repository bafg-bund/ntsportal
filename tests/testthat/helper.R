
connectNtsportal()

rootDirectoryForTestMsrawfiles <- "/beegfs/nts/ntsportal/msrawfiles/unit_tests"
ntspVersion <- "25.2"

testIndexName <- glue("ntsp{ntspVersion}_msrawfiles_unit_tests")

getEmptyRecord <- function() {
  emptyResult <- convertToDbasResult(emptyReport())
  convertToRecord(emptyResult, list(getRecordNoPeaks()))
}

checkForAlias <- function(record) {
  expect_match(record$dbas_alias_name, "ntsp\\d{2}\\.\\d+_dbas_unit_tests")
}

getOneSampleRecords <- function() {
  recs <- getMsrawfilesTestRecords()
  keep(recs, \(rec) grepl("KO_06_1_pos|BL_1_pos", rec$path))
}

getRecordNoPeaks <- function() {
  records <- getMsrawfilesTestRecords()
  allBatches <- splitRecordsByDir(records)
  allBatches[[grep("unit_tests/no-peaks", names(allBatches))]][[1]]
}

getMsrawfilesTestRecords <- function() {
  readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
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
