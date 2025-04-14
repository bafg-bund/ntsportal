
test_that("One file is uploaded in Elasticsearch", {
  
  esIndices <- ingestJson(test_path("fixtures", "screening-ingest", "exampleJson"))
  indexName <- esIndices[[1]]$ntsp
  expect_match(indexName, "ntsp\\d{2}.*_dbas_v\\d{12}_unit_tests")
  Sys.sleep(1)
  
  client <- getEsClient()
  expect_true(client$indices$exists(index=indexName)$body)
  resp <- client$count(index=indexName)
  expect_gt(resp$body$count, 0)
  resp <- client$search(index=indexName, size=1, source="dbas_alias_name")
  expect_match(resp$body$hits$hits[[1]][["_source"]]$dbas_alias_name, "ntsp\\d{2}\\.\\d+_dbas_unit_tests")
  
  deleteIndex(indexName)

})

