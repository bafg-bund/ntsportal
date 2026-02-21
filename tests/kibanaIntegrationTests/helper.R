
devtools::load_all()

getExampleDocs <- function(tableName) {
  getTableAsRecords(
    getDbComm(), 
    tableName, 
    searchBlock = list(
      query = list(
        bool = list(
          must = list(
            list(term = list(station = "donau_jo_m")),
            list(terms = list(name = list("Clotrimazole", "PEG-09"))),
            list(term = list(duration = 365))
          )
        )
      )
    ),
    sortField = "start", 
    recordConstructor = newFeatureRecord
  )
}

changeAliasInDocs <- function(docs, newAlias) {
  lapply(docs, function(x) {x[["dbas_alias_name"]] <- newAlias; x})
}

removeEicAndSpectra <- function(docs) {
  lapply(docs, function(doc) {
    #doc$ms1 <- NULL
    doc$ms2 <- NULL
    #doc$eic <- NULL
    doc
  })
}

addIntStdToDocs <- function(docs) {
  lapply(docs, function(doc) {
    doc$internal_standard <- if (doc$name == "Clotrimazole") "Bezafinbrate-d4" else c("other", "other2", "other3", "other4")
    doc
  })
}

ingestNewDocs <- function(newDocs) {
  newDir <- withr::local_tempdir()
  pathToJson <- file.path(newDir, "docsToIngest.json")
  
  writeRecord(newDocs, pathToJson)
  compressJson(pathToJson)
  indexNames <- ingestJson(newDir)
  indexNames
}