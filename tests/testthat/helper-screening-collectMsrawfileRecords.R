

entireTestMsrawfilesIndex <- function() {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  readRDS(test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}

prepareExampleFeatureIndex <- function() {
  indexNames <- elastic::cat_aliases(escon, index = "ntsp_dbas_unit_tests", parse = T)[,2]
  elastic::index_delete(escon, index = indexNames)
  emptyRecord <- readRDS(test_path("fixtures", "screening-savingRecord", "emptyRecord.RDS"))
}