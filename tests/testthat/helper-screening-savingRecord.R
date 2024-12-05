

getSimpleRecord <- function() {
  readRDS(test_path("fixtures", "screening-savingRecord", "oneSampleRecord.RDS"))
}

getEmptyRecord <- function() {
  readRDS(test_path("fixtures", "screening-savingRecord", "emptyRecord.RDS"))
}
