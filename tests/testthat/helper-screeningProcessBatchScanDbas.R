

getSingleRecordDes_07_01_pos <- function() {
  msr <- readRDS(test_path("fixtures", "screeningProcessBatchScanDbas", "whole_msrawfiles_docsList.RDS"))
  msr[[10]][["_source"]]
}