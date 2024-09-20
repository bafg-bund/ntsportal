


test_that("Set of two batches can be processed", {
  docsList <- readRDS(test_path("fixtures", "proc_rawfiles_nts","whole_msrawfiles_docsList.RDS"))
  dl2 <- get_unproc_batches(docsList, "nts")
  dl3 <- dl2[3:4]
  tempSaveDir <- withr::local_tempdir()


  x <- proc_batches_nts(dl3, tempSaveDir, coresTotal = 1, saveIntermed = T)

  expect_equal(length(list.files(tempSaveDir, full.names = T)), 7)

  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
  
  # source("~/connect-ntsp.R")
  # docsList <- get_msrawfiles(escon, "ntsp_index_msrawfiles_unit_tests")
  # saveRDS(docsList, test_path("fixtures", "proc_rawfiles_nts","whole_msrawfiles_docsList.RDS"))
})

test_that("Ingest entire nts folder of batches", {
  source("~/connect-ntsp.R")
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  
  put_nts_index(escon, "ntsp_index_nts_v20240510_unit_tests")
  es_move_alias(escon, "ntsp_index_nts_v20240510_unit_tests", "ntsp_nts_unit_tests")
  x <- ingest_all_batches(
    escon,
    rfindex = rfindex,
    resDir = test_path("fixtures", "processed_results_test"),
    configPath = "~/config.yml",
    ingestScriptPath = test_path("fixtures", "ingest.sh"),
    type = "nts",
    pauseTime = 2
  )
  elastic::alias_delete(escon, index = "ntsp_index_nts_v20240510_unit_tests", alias = "ntsp_nts_unit_tests")
  elastic::index_delete(escon, "ntsp_index_nts_v20240510_unit_tests")
})

test_that("Process whole msrawfiles for new files and create jsons", {
  docsList <- readRDS(test_path("fixtures", "whole_msrawfiles_docsList.RDS"))
  tempSaveDir <- withr::local_tempdir()
  
  
  proc_nts_new(msrawfiles = docsList, saveDir = tempSaveDir, coresTotal = 1)
  
  expect_equal(length(list.files(tempSaveDir)), 3)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
  
  # source("~/connect-ntsp.R")
  # rfindex <- "ntsp_index_msrawfiles_unit_tests"
  # msr <- get_msrawfiles(escon, rfindex)
  # saveRDS(msr, test_path("fixtures", "whole_msrawfiles_docsList.RDS"))
})

test_that("Collect all batches which are still open for processing", {
  docsList <- readRDS(test_path("fixtures", "whole_msrawfiles_docsList.RDS"))
  batches <- get_unproc_batches(docsList, "nts")
  expect_equal(length(batches), 4)
  expect_true(any(grepl("olmesartan-d6", names(batches))))
  x <- sapply(batches, length)["/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol"]
  expect_equal(as.numeric(x), 8)
  
  # For getting data
  # source("~/connect-ntsp.R")
  # rfindex <- "ntsp_index_msrawfiles_unit_tests"
  # reset_eval(escon, rfindex, list(match_all = stats::setNames(list(), character(0))), "nts", confirm = FALSE)
  # docsList <- es_search_paged(escon, rfindex, searchBody = list(query = list(match_all = stats::setNames(list(), character(0)))), sort = "path")$hit$hits
  # saveRDS(docsList, test_path("fixtures", "whole_msrawfiles_docsList.RDS"))
}) 

# Batch process functions ####

test_that("Peakpicking on a single file works", {
  
  ds <- jsonlite::read_json(
    test_path("fixtures", "doc_source-Des_07_02_pos.json")
  )
  
  result <- proc_doc_pp(ds)
  expect_equal(length(result), 3)
  expect_equal(nrow(result$pl), 16)
})

test_that("Peakpicking on a single file returns null when no peaks found", {
  
  ds <- jsonlite::read_json(
    test_path("fixtures", "doc_source-RH_pos_20220602_no_peaks.json")
  )
  
  result <- proc_doc_pp(ds)
  expect_null(result)
})

test_that("A batch (3 samples), with no blanks is processed to proc_output and
          finds IS and compound annotations", {
  
  dl <- jsonlite::read_json(
    test_path("fixtures", "doc_source-Des_07-batch.json")
  )
  
  po <- proc_batch_nts(
    docsList = dl,
    coresBatch = 1
  )
  
  expect_equal(length(po), 7)
  expect_equal(nrow(po$sampleList), 6)
  expect_contains(po$annotationTable$name, "Olmesartan-d6")
  expect_contains(po$annotationTable$name, "Bisoprolol")
  expect_s3_class(po, "proco_nts")
  
  # Bisoprolol on one row of alignment table
  alig <- as.data.frame(po$grouped[, c(1,2)])
  a2 <- subset(alig, abs(mean_mz - 326.233) <= .005 & abs(mean_RT / 60 - 7) <= .4)
  expect_equal(nrow(a2), 1)
  
  # source("~/connect-ntsp.R")
  # rx <- elastic::Search(
  #   escon, "ntsp_index_msrawfiles_unit_tests",
  #   body = list(
  #     query = list(
  #       bool = list(
  #         must = list(
  #           list(regexp = list(path = "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol/.*")),
  #           list(term = list(blank = FALSE))
  #         )
  #       )
  #     ),
  #     sort = list(list(path = "asc"))
  #   )
  # )$hits$hits
  # jsonlite::write_json(rx, test_path("fixtures", "doc_source-Des_07-batch.json"), pretty = T, auto_unbox = T)

})

test_that("A batch does not find the IS", {
  dl <- readRDS(
    test_path("fixtures", "proc_rawfiles_nts", "doclist_no_is.RDS")
  )
  
  po <- proc_batch_nts(
    docsList = dl,
    coresBatch = 1
  )
  expect_true(all(po$sampleList$normalizePeakID == 0))
})


test_that("A batch with blanks has compound annotations", {
  
  dl <- jsonlite::read_json(
    test_path("fixtures", "proc_rawfiles_nts", "doc_source-Des_07-batch-w-blanks.json")
  )
  
  po <- proc_batch_nts(
    docsList = dl,
    coresBatch = 1
  )  

  
  expect_equal(po$annotationTable$name, "Bisoprolol")
  expect_s3_class(po, "proco_nts")
  
  # source("~/connect-ntsp.R")
  # rx <- elastic::Search(
  #   escon, "ntsp_index_msrawfiles_unit_tests",
  #   body = list(
  #     query = list(
  #       bool = list(
  #         must = list(
  #           list(regexp = list(path = ".*olmesartan-d6-bisoprolol/.*"))
  #         )
  #       )
  #     ),
  #     sort = list(list(path = "asc"))
  #   )
  # )$hits$hits
  # jsonlite::write_json(rx, test_path("fixtures", "proc_rawfiles_nts", "doc_source-Des_07-batch-w-blanks.json"), pretty = T, auto_unbox = T)

  # saveRDS(po, test_path("fixtures", "proco_nts-des-batch-w-blank.RDS"))
  
})

test_that("A batch with one file that has no peaks still gets processed", {
  
  dl <- jsonlite::read_json(
    test_path("fixtures", "doc_source-Des_07-one-file-no-peaks-batch.json")
  )
  
  po <- proc_batch_nts(
    docsList = dl,
    coresBatch = 1
  )
  
  expect_contains(po$annotationTable$name, "Bisoprolol")
  expect_s3_class(po, "proco_nts")
})

# Batch processing is not done yet
# test_that("Parallel batch processing works", {
#   
#   dl <- jsonlite::read_json(
#     test_path("fixtures", "doc_source-Des_07-batch-w-blanks.json")
#   )
#   
#   suppressWarnings(
#     po <- proc_batch_nts(
#       docsList = dl,
#       coresBatch = 2
#     )  
#   )
#   
#   expect_equal(po$annotationTable$name, "Bisoprolol")
#   expect_s3_class(po, "proco_nts")
#   
# })

test_that("Consecutive filter removes a peak from the alignment table", {
  dlist <- readRDS(test_path("fixtures", "proc_rawfiles_nts", "consecutive-filer-docsList.RDS"))
  proco <- proc_batch_nts(dlist, 1)
  expect_equal(proco$grouped[, "alignmentID"], 2:3)
  
  # q <- list(regexp = list(path = ".*/olmesartan-d6/.*"))
  # srt <- list(list(start = "asc"))
  # dlist <- elastic::Search(escon, rfindex, body = list(query = q, sort = srt))$hits$hits
  # saveRDS(dlist, test_path("fixtures", "proc_rawfiles_nts", "consecutive-filer-docsList.RDS"))
})

# Generating ntspl object ####

test_that("A ntspl_nts can be generated from a proco_nts", {
  procoOb <- readRDS(test_path("fixtures", "proco_nts-des-batch-w-blank.RDS"))
  
  ntsplOb <- make_ntspl(procoOb, coresBatch = 1)
  
  expect_s3_class(ntsplOb, "ntspl_nts")
  expect_equal(attr(ntsplOb, "nts_alias_name"), "ntsp_nts_unit_tests")
  # All fields are only allowed to appear once or none in each list
  fields <- unique(unlist(lapply(ntsplOb, names)))
  moreThan1 <- any(sapply(fields, function(f) {
    any(sapply(lapply(ntsplOb, names), function(x) sum(x == f)) > 1)
  }))
  expect_false(moreThan1)
  expect_equal(length(ntsplOb), 283)
  f <- fs::path_package("ntsportal", "extdata", "nts_index_mappings.json")
  mappings <- jsonlite::read_json(f)$mappings$properties
  
  ch <- all(is.element(unique(unlist(lapply(ntsplOb, names))), names(mappings)))
  expect_true(ch)
  
  
  # saveRDS(ntsplOb, test_path("fixtures", "proc_rawfiles_nts","ntspl_nts-des-batch-w-blank.RDS"))
})


test_that("A ntspl_nts can be written to disk", {
  # This file has been created in the test "A ntspl_nts can be generated from a proco_nts
  ntsplOb <- readRDS(test_path("fixtures", "proc_rawfiles_nts","ntspl_nts-des-batch-w-blank.RDS"))
  tempSaveDir <- withr::local_tempdir()
  
  savePath <- save_ntspl(ntsplOb, tempSaveDir)
  expect_true(grepl("nts-batch", savePath))
  expect_equal(stringr::str_match(savePath, "--inda-(.*)-indn")[,2], "ntsp_nts_unit_tests")
  expect_true(file.exists(savePath))
  
  # rstudioapi::documentOpen(savePath)
  #expect_true(file.size(savePath) == 1442107)
  
  # Code to save the results for the next test "A json file can be ingested into a test index"
  # file.copy(savePath, test_path("fixtures", "proc_rawfiles_generic", basename(savePath)))
  # file.remove(paste0(test_path("fixtures", "proc_rawfiles_generic", basename(savePath)), ".gz"))
  # system2("gzip", test_path("fixtures", "proc_rawfiles_generic", basename(savePath)))

  file.remove(savePath)
  file.remove(tempSaveDir)
})



test_that("Add processing time for nts data to msrawfiles", {

  source("~/connect-ntsp.R")
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  
  dl <- jsonlite::read_json(
    test_path("fixtures", "doc_source-Des_07-batch.json")
  )
  
  ds <- lapply(dl, "[[", i = "_source")
  paths <- sapply(ds, "[[", i = "path")
  # clear fields
  x <- reset_eval(escon, rfindex, list(match_all = stats::setNames(list(), character(0))), "nts", confirm = FALSE)
  expect_true(x)
                                
  Sys.sleep(2)
  res <- record_proc(
    escon = escon, 
    rfindex = rfindex, 
    pathRawfiles = paths, 
    type = "nts"
  )
  expect_true(res)
  
})

