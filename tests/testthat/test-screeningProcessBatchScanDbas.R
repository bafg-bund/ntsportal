

test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol v2", {
  
  
  msr <- readRDS(test_path("fixtures", "screeningProcessBatchScanDbas", "whole_msrawfiles_docsList.RDS"))
  recordDes_07_01 <- msr[[10]][["_source"]]
  
  fileResult <- fileScanDbas(msrawfileRecord = recordDes_07_01)
  
})




test_that("Test file Des_07_01_pos.mzXML can be processed for Bisoprolol", {
  
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  rfloc <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/"
  
  t1 <- check_integrity_msrawfiles(escon, rfindex = rfindex, locationRf = rfloc)
  expect_true(t1)
  
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
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
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
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
  isInd <- "ntsp_is_dbas_unit_tests"
  rfInd <- "ntsp_index_msrawfiles_unit_tests"
  ingestP <- test_path("fixtures", "ingest.sh")
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
  
  # Should process 3 docs, 2 successfully, if this is more, then the test will fail.
  process_is_all(
    escon = escon,
    rfindex = rfInd,
    isindex = isInd,
    ingestpth = ingestP,
    configfile = configfile,
    tmpPath = tempsavedir,
    numCores = 1
  )
  #expect_equal(length(list.files(tempsavedir)), 1)
  numDocs <- elastic::Search(escon, isInd, body = '{"query": {"match_all": {}}}', 
                  size = 0)$hits$total$value
  expect_equal(numDocs, 2)
  file.remove(list.files(tempsavedir, full.names = T))
})

test_that("A file with no peaks should should not return an error", {
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
  tempsavedir <- withr::local_tempdir()
  # VNSTWpABQ5NoSyLHKzdl has no peaks, -8q2OpABQ5NoSyLH7DJR is a normal file
  res <- process_is_all(
    escon = escon, 
    rfindex = "ntsp_index_msrawfiles_unit_tests",
    isindex = "ntsp_is_dbas_unit_tests",
    ingestpth = test_path("fixtures", "ingest.sh"),
    configfile = "~/config.yml",
    tmpPath = tempsavedir, 
    numCores = 1,
    idsToProcess = c("X","VNSTWpABQ5NoSyLHKzdl","-8q2OpABQ5NoSyLH7DJR"),
    noIngest = T
  )
  expect_true(res)
  expect_equal(length(list.files(tempsavedir)), 1)
  file.remove(list.files(tempsavedir, full.names = T))
})


test_that("Test triplicate batch can be processed", {
  source("~/connect-ntsp.R")
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  tempsavedir <- withr::local_tempdir()
  coresBatch <- 4
  ingestpth <- test_path("fixtures", "ingest.sh")
  configfile <- "~/config.yml"
  
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
  
  # Delete old index
  oldIndex <- get_field(escon, rfindex, esids, "dbas_index_name", j = T)
  if (elastic::index_exists(escon, oldIndex))
    res6 <- elastic::index_delete(escon, oldIndex, verbose = F)
  
  # Create new index
  Sys.sleep(1)
  res1 <- create_dbas_index_all(escon = escon, rfindex = rfindex)
  
  expect_true(res1)
  Sys.sleep(1)
  # Reset eval
  reset_eval(
    escon, rfindex, 
    queryBody = list(ids = list(values = esids)), 
    indexType = "dbas", confirm = F
  )
  
  # Run processing
  
  numDocs <- proc_batch(escon, rfindex, esids, tempsavedir, ingestpth, configfile, coresBatch, noIngest = F)
  expect_equal(length(list.files(tempsavedir)), 3)
  expect_equal(numDocs, 33)
  
  # Update the alias
  res5 <- update_alias_all(escon = escon, rfindex = rfindex)
  expect_true(res5)
  # Check that the results are in Elastic
  res7 <- elastic::Search(
    escon, "ntsp_dbas_units_tests", 
    body = '{"query": {"match_all": {}}}', size = 0
  )
  expect_equal(res7$hits$total$value, 33)
  
  # Check that Nortilidin is not in the results, since this was only found in one replicate
  
  res3 <- elastic::Search(
    escon, "ntsp_dbas_units_tests", 
    body = list(query = list(term = list(name = "Nortilidin"))), size = 0
  )
  expect_equal(res3$hits$total$value, 0)
  
  # Check for dbas_last_eval field and make sure the date entered is not more than 1000 s in the past
  Sys.sleep(1)
  res8 <- elastic::Search(
    escon, rfindex, size = 0, 
    body = list(
      query = list(ids = list(values = esids)), 
      aggs = list(
        maxTime = list(max = list(field = "dbas_last_eval"))
      )
    )
  )
  
  expect_true(round(as.numeric(Sys.time()) - res8$aggregations$maxTime$value / 1000) < 1000)
  
  file.remove(list.files(tempsavedir, full.names = T))
})

