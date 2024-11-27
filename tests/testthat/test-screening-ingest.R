
test_that("One file is uploaded in Elasticsearch", {
  
  es_indeces = ingestJson(test_path("fixtures", "screening-ingest", "testfolder"))
  index_name = es_indeces[[1]]$ntsp_dbas_unit_tests
  
  Sys.sleep(1)
  connectNtsportal()
  library(elastic)
  expect_true(index_exists(escon, index_name))
 
  index_delete(escon, index_name)

})







