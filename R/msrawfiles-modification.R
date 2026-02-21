


#' Change alias of an index to a new index.
#' 
#' @description Will delete the previous alias
#'
#' @param indexName Name of index
#' @param aliasName Name of alias
#' @param closeAfter logical, should the previous index, to which the alias 
#' linked to prior to the change, be closed? 
#'
#' @return Passes on response from ES (acknowledged field, TRUE or FALSE), invisibly
changeAliasAddress <- function(indexName, aliasName, closeAfter = FALSE) {
  aliases <- elastic::cat_aliases(escon, index = aliasName, parse = T)
  # Delete previous alias
  if (!is.null(aliases)) {
    res1 <- elastic::alias_delete(escon, index = aliases[1, 2], alias = aliases[1, 1])
    previousIndex <- aliases[1, 2]
  }
  res <- elastic::alias_create(escon, indexName, aliasName)
  ok <- res$acknowledged
  if (ok) {
    logger::log_info("Success creating alias {aliasName} on index {indexName}")
  } else {
    logger::log_error("Error creating alias {aliasName} on index {indexName}")
  }
  if (ok && exists("previousIndex") && length(previousIndex) == 1 && closeAfter) {
    elastic::index_close(escon, previousIndex)
    logger::log_info("Closed index {previousIndex}")
  }
  invisible(res$acknowledged)
}

#' Change the path of one document in msrawfiles
#' @description
#' This is a secure method of changing the "path" field of an entry in msrawfiles. The filename is not changed and must
#' remain the same. It will check that both files are the same before making the change.
#' @param rfIndex index name for msrawfiles table
#' @param oldPath Current path in msrawfiles table
#' @param newPath New path
#' @param checkType Method to check that files are the same, either "md5" (default) or "filesize"
#' @details
#' Both files must exist when making the change. The function takes some time if it compares md5-checksums of the two
#' files. For large numbers of files use `checkType` = "filesize".
#' @export
#' @examples
#' \dontrun{
#' library(ntsportal)
#' connectNtsportal()
#' 
#' ind <- "ntsp_msrawfiles"
#' res <- esSearchPaged(ind, sort = "path", searchBody = list(query = list(regexp = list(path = "/srv.*"))), source = "path")$hits$hits
#' partToRemove <- "/srv/cifs-mounts/g2/G/G2/HRMS/Messdaten/"
#' paths <- sapply(res, function(x) x[["_source"]]$path)
#' oldPaths <- data.frame(oldPath = paths, relPath = sub(partToRemove, "", paths))
#' relPaths <- list.files("/beegfs/nts/ntsportal/msrawfiles", recursive = T)
#' newPaths <- data.frame(
#'   newPath = paste0("/beegfs/nts/ntsportal/msrawfiles/", relPaths),
#'   relPath = relPaths
#' )
#' 
#' oldAndNewPaths <- merge(oldPaths, newPaths, all.x = T, by = "relPath")
#' 
#' for (i in 1:nrow(oldAndNewPaths)) {
#'   changeMsrawfilePath(ind, oldAndNewPaths$oldPath[i], oldAndNewPaths$newPath[i], checkType = "filesize")  
#' }
#' }
changeMsrawfilePath <- function(rfIndex, oldPath, newPath, checkType = "md5") {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(all(file.exists(oldPath, newPath)))
  stopifnot(checkType %in% c("md5", "filesize"))
  stopifnot(basename(oldPath) == basename(newPath))
  # Same content (slow)
  if (checkType == "md5" && tools::md5sum(oldPath) != tools::md5sum(newPath))
    stop(oldPath, " and ", newPath, " are not the same")
  # Same size (faster)
  if (checkType == "filesize" && file.size(oldPath) != file.size(newPath)) {
    stop(oldPath, " and ", newPath, " are not the same")
  }
  normOldPath <- normalizePath(oldPath)
  normNewPath <- normalizePath(newPath)
  numOldPath <- getNrecsWithPath(rfIndex, normOldPath)
  numNewPath <- getNrecsWithPath(rfIndex, normNewPath)
  if (numOldPath == 1 && numNewPath == 0) {
    replaceValueInField(getDbComm(), rfIndex, "path", normOldPath, normNewPath)
  } else {
    stop("Database not in the correct state for a change. Number of records with oldPath: ", numOldPath, ". Number of
         records with newPath: ", numNewPath)
  }
}

#' Change filename in the msrawfiles-db and on the filesystem
#' @description Used to change filename of a measurement file and simultaneously
#' change the `path` field in `msrawfiles`. This change is riskier than using `changeMsrawfilePath` so it isn't exported 
#' @inheritParams changeMsrawfilePath
changeMsrawfileFilename <- function(rfIndex, oldPath, newPath) {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(file.exists(oldPath))
  normOldPath <- normalizePath(oldPath)
  numOldPath <- getNrecsWithPath(rfIndex, normOldPath)
  numNewPath <- getNrecsWithPath(rfIndex, newPath)
  if (numOldPath == 1 && numNewPath == 0) {
    file.rename(from = normOldPath, to = newPath)
    normNewPath <- normalizePath(newPath)
    replaceValueInField(getDbComm(), rfIndex, "path", normOldPath, normNewPath)
  } else {
    stop("Database not in the correct state for a change. Number of records with oldPath: ", numOldPath, ". Number of
         records with newPath: ", numNewPath)
  }
}

getNrecsWithPath <- function(rfIndex, normPath) {
  queryPath <- list(query = list(term = list(path = normPath)))
  getNrow(getDbComm(), rfIndex, searchBlock = queryPath)
}

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
