


entireTestMsrawfilesIndex <- function() {
  readRDS(test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}

prepareExampleFeatureIndex <- function(escon) {
  indexNames <- elastic::cat_aliases(escon, index = "ntsp_dbas_unit_tests", parse = T)[,2]
  if (!is.null(indexNames))
    elastic::index_delete(escon, index = indexNames)
  emptyResult <- convertToDbasResult(emptyReport())
  emptyRecord <- convertToRecord(emptyResult, list(getMsrawfileRecordNoPeaks()))
  tempDir <- withr::local_tempdir()
  saveRecord(emptyRecord, tempDir)
  ingestJson(tempDir)
  Sys.sleep(1)
  file.remove(list.files(tempDir, f = T))
  file.remove(tempDir)
}

removeExampleFeatureIndex <- function(escon) {
  indexNames <- elastic::cat_aliases(escon, index = "ntsp_dbas_unit_tests", parse = T)[,2]
  if (!is.null(indexNames))
    elastic::index_delete(escon, index = indexNames)
}
