

entireTestMsrawfilesIndex <- function() {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  readRDS(test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}