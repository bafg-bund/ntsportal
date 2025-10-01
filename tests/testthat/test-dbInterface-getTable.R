
test_that("You can retrieve an index of two fields as a tibble", {
  testTibble <- getTableByQuery(testIndexName, fields = c("path", "start"))
  expect_s3_class(testTibble, "tbl_df")
  expect_gte(nrow(testTibble), 20)
  expect_length(testTibble, 2)
})

test_that("A nested field is returned as a list column of tibbles", {
  testTibble <- getTableByQuery(
    "ntsp25.2_spectral_library", 
    searchBlock = list(query = list(term = list(name = "Carbamazepine"))), 
    fields = c("name", "rtt")
  )
  oneSpec <- purrr::pluck(testTibble, "rtt", 1)
  expect_s3_class(oneSpec, "tbl_df")
})

test_that("Records with different length elements are still coerced into a tibble", {
  minimalRecords <- list(
    list(comp_group = c("A", "B")),
    list(comp_group = "A")
  )
  newTib <- convertRecordsToTibble(minimalRecords)
  expectedTib <- tibble(comp_group = c(list(c("A", "B")), list("A")))
  expect_identical(newTib, expectedTib)
})

test_that("You can get a tibble from and ES|QL", {
  testTbl <- getTableByEsql("FROM ntsp25.1_dbas* | WHERE station == \"mosel_139\" | KEEP mz, station | LIMIT 10")
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
