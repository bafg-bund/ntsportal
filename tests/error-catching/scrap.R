

udb <- udb_connect("tests/ufid_alignment/aug-test/ufid-lib-aug-test.sqlite")


library(dplyr)
library(logger)
tbl(udb, "feature") %>% filter(ufid == 544)


DBI::dbExecute(udb, '
               DELETE FROM feature
               WHERE
                 ufid == 544;')




resE <- try(elastic::docs_update_by_query(escon, index, body = '
                              {
  "query": {
    "match_all": {}
  },
  "script": {
    "source": "throw new Exception(\'this is a test\');",
    "lang": "painless"
  }
}
                              '))
blah <- function() {
  elastic::docs_update_by_query(escon, index, body = '
                              {
  "query": {
    "match_all": {}
  },
  "script": {
    "source": "throw new Exception(\'this is a test\');",
    "lang": "painless"
  }
}
                              ')
}
resT <- tryCatch(
  blah(),
  error = function(e) {
    log_info("an error has occurred")
  }
)


str(resE)
if (inherits(resE, "try-error"))
  log_info("error in ")

elastic::count(escon, index)


source("~/connect-ntsp.R")

queryBody <- '
                  {
    "query": {
      "bool": {
        "must": [
          {
            "term": {
              "name": {
                "value": "Carbamazepine"
              }
            }
          }
        ]
      }
    },
    "size": 20000
  }
                  '

tryCatch(
  res10 <- elastic::Search(escon, "g2_nts_bfg", body = queryBody),
  error = function(cnd) {
    browser()
    log_error("this is an error {cnd}")
  }
)

es_break <- function(thisCnd) {
  if (is.character(thisCnd$message) && grepl("429", thisCnd$message)) {
    logger::log_warn("429 error from Elastic, taking a 2 minute break")
    Sys.sleep(120)
  }
}  


tryCatch(
  res10 <- elastic::Search(escon, "g2_nts_bfg", body = queryBody),
  error = function(cnd) es_print_error(cnd, queryBody)
)

message("code continues")


cnd <- rlang::catch_cnd(elastic::Search(escon, "g2_nts_bfg", body = queryBody))
str(cnd)
testit <- function(blah)
  cat("blah")
es_print_error <- function(errorText, queryText) {
  esql <- function(q) {
    stringr::str_squish(stringr::str_replace_all(q, "[\\r\\n]", ""))
  }
  logger::log_error("There was an error: {errorText}")
  logger::log_error("The original call was: {esql(queryText)}")
}

library(logger)
giveTrue <- function() {
  stop("bad shit")
  TRUE
}
blah <- FALSE
tryCatch(
  blah <- giveTrue(),
  error = function(cnd) log_error("There was an error: {skip_formatter(cnd)} but moving on")
)
exists("blah")
blah
cat("we are moving on")




