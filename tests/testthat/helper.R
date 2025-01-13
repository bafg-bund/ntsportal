
connectNtsportal()

rootDirectoryForTestMsrawfiles <- "/beegfs/nts/ntsportal/msrawfiles/unit_tests"

testIndexName <- "ntsp_msrawfiles_unit_tests"

getEmptyRecord <- function() {
  emptyResult <- convertToDbasResult(emptyReport())
  convertToRecord(emptyResult, list(getMsrawfileRecordNoPeaks()))
}
