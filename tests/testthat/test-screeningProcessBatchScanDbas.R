
connectNtsportal()

test_that("Test triplicate batch can be processed and json produced", {
  
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  tempsavedir <- withr::local_tempdir()
  coresBatch <- 4
  
  res3 <- elastic::Search(
    escon, rfindex, size = 100, 
    body = list(
      query = list(
        regexp = list(
          path = ".*bisoprolol.*"
        )
      ),
      sort = list(
        list(
          start = "asc"
        )
      )
    ), source = F)
  esids <- sapply(res3$hits$hits, function(x) x[["_id"]])
  
  # Reset eval
  reset_eval(
    escon, rfindex, 
    queryBody = list(ids = list(values = esids)), 
    indexType = "dbas", confirm = F
  )
  
  # Run processing
  
  numDocs <- proc_batch(escon, rfindex, esids, tempsavedir, ingestpth, configfile, coresBatch, noIngest = F)
  
  # Test
  expect_equal(length(list.files(tempsavedir)), 3)
  expect_equal(numDocs, 33)
  
  # Check that Nortilidin is not in the results, since this was only found in one replicate
  
  jsonPath <- list.files(tempsavedir, pattern = "json$", full.names = T)
  
  results <- jsonlite::read_json(jsonPath)
  
  compsFound <- unique(gf(results, "name", character(1)))
  expect_false(is.element("Nortilidin", compsFound))
  
  # Clean up
  file.remove(list.files(tempsavedir, full.names = T))
  file.remove(tempsavedir)
})



test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol v2", {
  recordDes_07_01 <- getSingleRecordDes_07_01_pos()
  
  fileResult <- fileScanDbas(msrawfileRecord = recordDes_07_01, compsToProcess = "Bisoprolol")
  
  expect_s4_class(fileResult, "Report")
  expect_equal(fileResult$peakList$comp_name, "Bisoprolol")
})

test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol (old version)", {
  
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  res1 <- elastic::Search(
    escon, rfindex, 
    body = list(query = list(term = list(filename = "Des_07_01_pos.mzXML"))), 
    source = "_id"
  )
  e <- res1$hits$hits[[1]][["_id"]]
  
  re <- proc_esid(
    escon = escon,
    rfindex = rfindex,
    esid = e,
    compsProcess = "Bisoprolol"
  )
  expect_equal(re$peakList$comp_name, "Bisoprolol")
})


test_that("Test file BW1_Dessau_pos.mzXML can be processed for IS Olmesartan-d6", {
  
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  res1 <- elastic::Search(
    escon, rfindex, 
    body = list(query = list(term = list(filename = "BW1_Dessau_pos.mzXML"))), 
    source = "_id")
  
  e <- res1$hits$hits[[1]][["_id"]]

  re <- proc_is_one(
    escon = escon,
    rfindex = rfindex,
    esid = e
  )
  expect_equal(re[[1]]$name, "Olmesartan-d6")
})

test_that("Test multiple files can be processed for IS", {
 
  isInd <- "ntsp_is_dbas_unit_tests"
  rfInd <- "ntsp_index_msrawfiles_unit_tests"
  ingestP <- fs::path_package("ntsportal", "scripts", "ingest.sh")
  rawFilesRoot <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/"
  tempsavedir <- withr::local_tempdir()
  configfile <- "~/config.yml"
  
  res3 <- elastic::Search(
    escon, rfInd, size = 100, 
    body = list(
      query = list(term = list(matrix = "water")),
      sort = list(
        list(
          start = "asc"
        )
      )
    ), source = F)
  esids <- sapply(res3$hits$hits, function(x) x[["_id"]])
  
  # Clear old docs
  elastic::docs_delete_by_query(escon, isInd, body = '{"query": {"match_all": {}}}')
  
  # Reset eval
  reset_eval(
    escon, rfInd, 
    queryBody = list(ids = list(values = esids)), 
    indexType = "dbas_is", confirm = F
  )
  Sys.sleep(1)
  process_is_all(
    escon = escon,
    rfindex = rfInd,
    isindex = isInd,
    ingestpth = ingestP,
    configfile = configfile,
    tmpPath = tempsavedir,
    rawfilesRootPath = rawFilesRoot,
    numCores = 1
  )
  Sys.sleep(1)
  numDocs <- elastic::Search(escon, isInd, body = '{"query": {"match_all": {}}}', 
                  size = 0)$hits$total$value
  expect_equal(numDocs, 4)
  file.remove(list.files(tempsavedir, full.names = T))
  file.remove(tempsavedir)
})

test_that("A file with no peaks should should not return an error", {

  tempsavedir <- withr::local_tempdir()
  # VNSTWpABQ5NoSyLHKzdl has no peaks, -8q2OpABQ5NoSyLH7DJR is a normal file
  res <- process_is_all(
    escon = escon, 
    rfindex = "ntsp_index_msrawfiles_unit_tests",
    isindex = "ntsp_is_dbas_unit_tests",
    ingestpth = test_path("fixtures", "ingest.sh"),
    configfile = "~/config.yml",
    tmpPath = tempsavedir, 
    rawfilesRootPath = "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files",
    numCores = 1,
    idsToProcess = c("X","VNSTWpABQ5NoSyLHKzdl","-8q2OpABQ5NoSyLH7DJR"),
    noIngest = T
  )
  expect_true(res)
  expect_equal(length(list.files(tempsavedir)), 1)
  file.remove(list.files(tempsavedir, full.names = T))
  file.remove(tempsavedir)
})



