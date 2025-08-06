
#' Update all ingest policies on the elasticsearch cluster
#' @export
updateIngestPipelines <- function() {
  pipelineNames <- getIngestPipelineNames()
  for (pipName in pipelineNames) {
    if (testForPipeline(pipName))
      deleteIngestPipeline(pipName)
    createIngestPipeline(pipName)
  }
  if (all(vapply(pipelineNames, testForPipeline, logical(1))))
    message("All ingest pipelines updated successfully")
}

getIngestPipelineNames <- function() {
  pipelineNames <- list.files(fs::path_package("ntsportal", "ingestPipelines"), pattern = "\\.json$")
  stringr::str_remove(pipelineNames, "\\.json$")
}

createIngestPipeline <- function(pipelineName) {
  dbComm <- getDbComm()
  processorContent <- readPipelineProcessors(pipelineName)
  res <- dbComm@client$ingest$put_pipeline(id=pipelineName, processors=processorContent)
  if (res$body$acknowledged) 
    message("Ingest pipeline ", pipelineName, " successfully created") 
}

readPipelineProcessors <- function(pipelineName) {
  pth <- fs::path_package("ntsportal", "ingestPipelines", glue("{pipelineName}.json"))
  wholeFile <- readr::read_file(pth, locale = readr::locale(encoding="UTF-8"))
  wholeFile <- stringr::str_replace_all(wholeFile, "\\n", "")
  asList <- RJSONIO::fromJSON(wholeFile, simplify = F)
  asList$processors
}

deleteIngestPipeline <- function(pipelineName) {
  dbComm <- getDbComm()
  res <- dbComm@client$ingest$delete_pipeline(id=pipelineName)
  if (res$body$acknowledged) 
    message("Ingest pipeline ", pipelineName, " successfully deleted") 
}

testForPipeline <- function(pipelineName) {
  dbComm <- getDbComm()
  res <- tryCatch(
    dbComm@client$ingest$get_pipeline(id = pipelineName), 
    error = function(cnd) {
      message(pipelineName, " not found: ", conditionMessage(cnd))  
    })
  if (exists("res")) {
    length(res$body[[pipelineName]]) == 1
  } else {
    FALSE
  }
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
