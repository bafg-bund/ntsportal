

test_that("The annotation inspector finds annotations and warns of missing annotations", {
  aliasIndexPair <- ingest(test_path("fixtures", "featureRecordExampleJson"))
  indexName <- aliasIndexPair[[1]]$ntsp
  expect_snapshot(
    inspectAnnotations(test_path("fixtures", "anotationInspectorTestTable.csv"), indexName)
  )
  
  dbComm <- getDbComm()
  deleteTable(dbComm, indexName)
})
