


entireTestMsrawfilesIndex <- function() {
  readRDS(test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}

prepareExampleFeatureIndex <- function(escon, ntspVersion) {
  removeExampleFeatureIndex(escon, ntspVersion)
  emptyResult <- convertToDbasResult(emptyReport())
  emptyRecord <- convertToRecord(emptyResult, list(getMsrawfileRecordNoPeaks()))
  tempDir <- withr::local_tempdir()
  saveRecord(emptyRecord, tempDir)
  ingestJson(tempDir)
}

removeExampleFeatureIndex <- function(escon, ntspVersion) {
  indexNames <- elastic::cat_aliases(escon, index = glue("ntsp{ntspVersion}_dbas_unit_tests"), parse = T)[,2]
  if (!is.null(indexNames))
    for (indexName in indexNames) 
      elastic::index_delete(escon, index = indexName)
}
