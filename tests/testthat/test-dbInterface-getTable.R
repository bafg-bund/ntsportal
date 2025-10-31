
test_that("You can retrieve an index of two fields as a tibble", {
  testTibble <- getTableByQuery(testIndexName, fields = c("path", "start"))
  expect_s3_class(testTibble, "tbl_df")
  expect_gte(nrow(testTibble), 20)
  expect_length(testTibble, 2)
})

test_that("Nested fields are coerced to multiple list-columns of tibbles, multi-length vectors are concatenated", {
  testTibble <- getTableByQuery(
    "ntsp25.2_spectral_library", 
    searchBlock = list(query = list(term = list(name = "Carbamazepine"))),
    fields = c("mz", "comp_group", "ms2", "rtt")
  )
  rttTbl <- purrr::pluck(testTibble, "rtt", 1)
  expect_s3_class(rttTbl, "tbl_df")
  ms2Tbl <- purrr::pluck(testTibble, "ms2", 1)
  expect_s3_class(ms2Tbl, "tbl_df")
  comp_groupVec <- purrr::pluck(testTibble, "comp_group", 1)
  expect_type(comp_groupVec, "character")
  expect_type(testTibble[1, "mz"], "double")
})

test_that("Records with different length elements are still coerced into a tibble", {
  minimalRecords <- list(
    list(comp_group = c("A", "B"), mz = 100.1),
    list(comp_group = "A", mz = 100.2)
  )
  newTib <- convertRecordsToTibble(minimalRecords)
  expectedTib <- tibble(comp_group = c(list(c("A", "B")), list("A")), mz = c(100.1, 100.2))
  expect_identical(newTib, expectedTib)
})

test_that("Numeric arrays are coerced to characters", {
  minimalRecords <- list(
    list(numeric_field = c(1, 2)),
    list(numeric_field = 1)
  )
  newTib <- convertRecordsToTibble(minimalRecords)
  expectedTib <- tibble(numeric_field = c(list(c(1, 2)), list(1)))
  expect_identical(newTib, expectedTib)
})

test_that("You can get a tibble from and ES|QL", {
  testTbl <- getTableByEsql("FROM ntsp25.2_dbas* | WHERE station == \"mosel_139\" | KEEP mz, station | LIMIT 10")
  expect_s3_class(testTbl, "tbl_df")
  expect_gte(nrow(testTbl), 10)
  expect_type(testTbl$mz, "double")
  expect_type(testTbl$station, "character")
  expect_length(testTbl, 2)
})

test_that("An ESQL with null values can still be loaded", {
  testTbl <- getTableByEsql('FROM ntsp25.2_dbas* | 
WHERE station == "mosel_ko_r" AND name == "Bentazone" |
WHERE start >= "2022-08-10" AND start <= "2022-08-12" |
KEEP name, start, area_internal_standard')
  expect_type(testTbl$area_internal_standard, "double")
  expect_true(any(is.na(testTbl$area_internal_standard)))
  expect_length(testTbl, 3)
})
