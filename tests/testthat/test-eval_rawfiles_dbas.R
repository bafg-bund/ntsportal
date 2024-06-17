test_that("Test file KO_06_1_pos.mzXML can be processed for CBZ", {
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
  
  re <- proc_esid(
    escon = escon,
    rfindex = "ntsp_index_msrawfiles_unit_tests",
    esid = "dd8ohI4BbGGk3ENz_S5y",
    compsProcess = "Carbamazepine"
  )
  expect_contains(re$peakList$comp_name, "Carbamazepine")
})

test_that("Test file KO_06_1_pos.mzXML can be processed for IS", {
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
  
  re <- proc_is_one(
    escon = escon,
    rfindex = "ntsp_index_msrawfiles_unit_tests",
    esid = "dd8ohI4BbGGk3ENz_S5y"
  )
  expect_equal(re[[1]]$name, "Olmesartan-d6")
})

# test_that("Test file KO_06_1_pos.mzXML can be processed for IS", {
#   logger::with_log_threshold(
#     source("~/connect-ntsp.R"),
#     threshold = "OFF"
#   )
#   
#   process_is_all(
#     escon = escon, 
#     rfindex = "ntsp_msrawfiles_unit_tests",
#     isindex = "ntsp_alias_is_dbas_bfg",
#     ingestpth = "scripts/ingest.sh",
#     configfile = "~/config.yml",
#     tmpPath = "/scratch/nts/tmp", 
#     numCores = 10
#   )
#   expect_equal(re[[1]]$name, "Olmesartan-d6")
# })