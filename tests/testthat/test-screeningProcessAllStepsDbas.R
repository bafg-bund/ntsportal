# Test for screeningProcessAllStepsDbas.R


test_that("process dbas all steps", {
  # 1. Prepare
  # Problems: 
  #   - Paths are Simlinks -> Need to be fixtures (CSL, tempsave)
  
  RFINDEX <- "ntsp_index_msrawfiles_unit_tests"  # RFINDEX <- "ntsp_msrawfiles"
  SPECLIBPATH <- "~/ntsgz/db/ntsp/spectral-lib/CSL_v24.3.db"
  TEMPSAVE <- withr::local_tempdir()
  CONFG <- "~/config.yml"
  INGESTPTH <- fs::path_package("ntsportal", "scripts", "ingest.sh")
  UPDATESPECDB <- "scripts/update-spectral-library-ntsp.R"
  ADDANALYSIS <- "scripts/compute-analysis-index.R"
  ROOTDIR_RF <- "~/messdaten"
  VERSION <- "2024-10-30"
  CORES <- 1
  CORESBATCH <- 6
  
  source("~/connect-ntsp.R")
  

  # Reset field "dbas_index_name" in test-msrawfiles
  # This only works if no index has been created that day
  ntsportal::create_dbas_index(escon, RFINDEX, "ntsp_dbas_unit_tests", 241105)
  
  # Remove the entries in the field "dbas_last_eval" to mark them for reprocessing
  ntsportal::reset_eval(
    escon, 
    RFINDEX, 
    queryBody = list(match_all = stats::setNames(list(), character(0))
                     ),
    confirm = F
  )
  
  # 2. Call the function
  screeningProcessAllStepsDbas(RFINDEX, SPECLIBPATH, TEMPSAVE, CONFG, INGESTPTH, 
                               UPDATESPECDB, ADDANALYSIS, ROOTDIR_RF, VERSION, 
                               CORES, CORESBATCH)
  
  
  
  # 3. Test
  numHits <- elastic::Search(escon, "ntsp_index_dbas_v241105_unit_tests", source = F, size = 0)$hits$total$value
  expect_equal(numHits, 128)
  
  
  # Clean up
  elastic::index_delete(escon, "ntsp_index_dbas_v241105_unit_tests")
  file.remove(list.files(tempsavedir, full.names = T))
  file.remove(tempsavedir)
})





