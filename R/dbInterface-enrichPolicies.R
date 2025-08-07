

#' Update enrich policies in Elasticsearch
#' @description
#' Refresh (overwrite and re-execute) the enrich policies as stored in the ntsportal package (inst/enrichPolicies)
#' @return a vector of task IDs for all enrich policy execution tasks
#' @export
updateEnrichPolicies <- function() {
  pipelineNames <- getIngestPipelineNames()
  for (pipName in pipelineNames) {
    if (testForPipeline(pipName))
      deleteIngestPipeline(pipName)
  }
  policyNames <- getEnrichPolicyNames()
  taskIds <- character()
  for (poliName in policyNames) {
    if (testForEnrichPolicy(poliName))
      deleteEnrichPolicy(poliName)
    Sys.sleep(1)
    createEnrichPolicy(poliName)
    if (testForEnrichPolicy(poliName)) {
      taskId <- executeEnrichPolicy(poliName)
      taskIds <- append(taskIds, taskId)
    }
  }
  walk(pipelineNames, createIngestPipeline)
  taskIds
}

getEnrichPolicyNames <- function() {
  policyNames <- list.files(fs::path_package("ntsportal", "enrichPolicies"), pattern = "\\.json$")
  stringr::str_remove(policyNames, "\\.json$")
}

createEnrichPolicy <- function(policyName) {
  dbComm <- getDbComm()
  policyContent <- jsonlite::read_json(fs::path_package("ntsportal", "enrichPolicies", glue("{policyName}.json")))
  res <- dbComm@client$enrich$put_policy(name=policyName, body=policyContent)
  if (res$body$acknowledged) 
    message("Enrich policy ", policyName, " successfully added") 
}

deleteEnrichPolicy <- function(policyName) {
  dbComm <- getDbComm()
  res <- dbComm@client$enrich$delete_policy(name=policyName)
  if (res$body$acknowledged) 
    message("Enrich policy ", policyName, " successfully deleted") 
}

testForEnrichPolicy <- function(policyName) {
  dbComm <- getDbComm()
  res <- dbComm@client$enrich$get_policy(name=policyName)
  if (length(res$body$policies) == 0) 
    return(FALSE)
  length(res$body$policies[[1]]) == 1  
}

#' Refresh an existing enrich policy
#' @param policyName Name of policy
#' @param asBackgroundTask Boolean, should it run in the background?
#' @return Task ID
#' @export
executeEnrichPolicy <- function(policyName, asBackgroundTask = TRUE) {
  dbComm <- getDbComm()
  res <- dbComm@client$enrich$execute_policy(name = policyName, wait_for_completion=!asBackgroundTask)
  res$body$task
}

cancelTask <- function(taskId) {
  dbComm <- getDbComm()
  res <- dbComm@client$tasks$cancel(task_id=taskId)
  if (res$body$nodes[[1]]$tasks[[taskId]]$cancelled)
    message("task cancelled")
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
