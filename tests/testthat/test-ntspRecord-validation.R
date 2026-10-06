

test_that("Full validation of msrawfiles runs without error", {
  expect_no_error(checkMsrawfiles(testIndexName))
})

test_that("Normal record validation returns true for dbas", {
  records <- getMsrawfilesTestRecords("dbas")
  expect_true(validateRecordsMsrawfiles(records))
})

test_that("Normal record validation returns true for nts", {
  records <- getMsrawfilesTestRecords("nts")
  expect_true(validateRecordsMsrawfiles(records))
})

test_that("An incorrect file path returns a warning", {
  records <- getMsrawfilesTestRecords("dbas")
  records[[10]]$path <- "foobar"
  expect_warning(filesExist(records[[10]]))
  expect_warning(correctRawfileLocation(records[[10]]$path))
})

test_that("A missing path returns a warning", {
  records <- getMsrawfilesTestRecords("dbas")
  records[[10]]$path <- NULL
  expect_warning(fieldsExistForSampleType(records[[10]]))
})

test_that("Wrong IS table returns a warning", {
  records <- getMsrawfilesTestRecords("dbas")
  records[[10]]$pol <- "neg"
  expect_warning(correctIsTablePolarity(records[[10]]))
})

test_that("Wrong replicate regex returns a warning", {
  records <- getMsrawfilesTestRecords("dbas")
  records[[10]]$replicate_regex <- "0[123]_pos"
  expect_warning(correctReplicateRegex(records[[10]]))
  
  records[[10]]$replicate_regex <- "0[567](_pos)"
  expect_warning(correctReplicateRegex(records[[10]]))
})

test_that("Wrong blank returns a warning", {
  records <- getMsrawfilesTestRecords("dbas")
  indexNonBlank <- which(!getField(records, "blank"))[1]
  records[[indexNonBlank]]$blank <- TRUE
  expect_warning(correctBlankRegex(records[[indexNonBlank]]))
})

test_that("An incorrect field in featureRecord in a nested field results in an error", {
  rr <- getOneSampleDbasResultAndRecords()
  oneFeatureRecord <- convertToRecord(rr$dbasResult, rr$msrRecords)[[1]]
  expect_true(validateRecord(oneFeatureRecord))
  oneFeatureRecord$ms2 <- c(oneFeatureRecord$ms2, error_field = "error")
  expect_warning(validateRecord(oneFeatureRecord), "ms2.*error_field")
  expect_false(suppressWarnings(validateRecord(oneFeatureRecord)))
})


test_that("Depending on the subclass of msrawfiles, different fields are required", {
  recDbas <- getMsrawfilesTestRecords("dbas")[[1]]
  recNts <- getMsrawfilesTestRecords("nts")[[1]]
  fieldsDbas <- defineRequiredFieldsAnySample(recDbas)
  fieldsNts <- defineRequiredFieldsAnySample(recNts)
  expect_gt(length(setdiff(fieldsDbas, fieldsNts)), 1)
  expect_contains(intersect(fieldsDbas, fieldsNts), "path")
})

test_that("An additional_blank_path pointing to a non-existant record returns a validation error", {
  recDbas <- getMsrawfilesTestRecords("dbas")
  blankPaths <- getField(recDbas, "additional_blank_path")
  expect_no_error(validateRecordsMsrawfiles(recDbas))
  firstNa <- which(sapply(blankPaths, \(x) is.na(x[1])))[1]
  firstThree <- which(sapply(blankPaths, length) == 3)[1]
  recDbas[[firstNa]]$additional_blank_path <- "foo"
  recDbas[[firstThree]]$additional_blank_path[3] <- "bar"
  expect_error(suppressWarnings(validateRecordsMsrawfiles(recDbas)))
})
