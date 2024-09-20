

test_that("A json file can be ingested into a test index", {
  
  source("~/connect-ntsp.R")
  
  jf <- list.files(test_path("fixtures", "proc_rawfiles_generic"), pattern = "^nts-batch--inda-ntsp_nts_unit_tests-indn-.*-bi-.*olmesartan-d6-bisoprolol.*\\.json.gz$", full.names = T)
  tempSaveDir <- withr::local_tempdir()
  njf <- file.path(tempSaveDir, basename(jf))
  file.copy(jf, njf)
  system2("gunzip", njf)
  njf <- stringr::str_match(njf, "(.*)\\.gz")[,2]
  
  # Create test index
  indexName <- stringr::str_match(njf, "-indn-(ntsp_index_nts.*)-bi-")[,2]
  aliasName <- stringr::str_match(njf, "-inda-(ntsp_nts.*)-indn-")[,2]
  put_nts_index(escon, indexName)
  es_move_alias(escon, indexName, aliasName)
  success <- FALSE
  tryCatch(
    success <- ingest_ntspl(
      escon, 
      ntsplJsonPath = njf, 
      configPath = "~/config.yml", 
      ingestScriptPath = fs::path_package("ntsportal", "scripts", "ingest.sh"),
      pauseTime = 2,
      verbose = F
    ),
    error = function(cnd) {
      log_info("error at ingest: {conditionMessage(cnd)}")
    },
    finally = {
      elastic::alias_delete(escon, indexName, aliasName)
      elastic::index_delete(escon, indexName)
      file.remove(list.files(tempSaveDir, full.names = T))
      file.remove(tempSaveDir)
    }
  )
  expect_true(success)
  expect_false(elastic::index_exists(escon, aliasName))
  expect_false(elastic::index_exists(escon, indexName))
  expect_false(dir.exists(tempSaveDir))
})


test_that("New indexes can be created for nts processing", {
  source("~/connect-ntsp.R")
  
  create_index_all(escon, rfindex = "ntsp_index_msrawfiles_unit_tests", type = "nts", dateNum = "240905")
  
  expect_true(
    elastic::index_exists(escon, "ntsp_index_nts_v240905_unit_tests")
  )
  res <- elastic::index_delete(escon, index = "ntsp_index_nts_v240905_unit_tests")
  expect_true(res$acknowledged)
  
})

test_that("Collect whole msrawfiles and make a subset", {
  source("~/connect-ntsp.R")
  
  x <- get_msrawfiles(escon, "ntsp_index_msrawfiles_unit_tests")
  class(x)
  expect_s3_class(x, "msrawfiles_docs_list")
  # This is not working yet for whatever reason
  # sloop::s3_dispatch(x[1:3])
  # y <- x[1:3]
  # class(y)
})