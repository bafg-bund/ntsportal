
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
#' library(dplyr)
#' library(tibble)
#' rfIndex <- "ntsp26.1_msrawfiles"
#' 
#' createBackupMsrawfiles(rfIndex)
#' 
#' oldPath <- getTableByEsql(
#'   'FROM ntsp26.1_msrawfiles | 
#'   WHERE station == "rhein_bi_l" AND matrix == "spm" AND pol == "pos" AND duration == "P1Y" | 
#'   KEEP path')
#' 
#' oldPath$basename <- basename(oldPath$path)
#' 
#' dirs <- list.files("/beegfs/nts/ntsportal/msrawfiles/rhein_bimmen_l/spm/p1y", pattern = "pos$", include.dirs = T, full.names = T, recursive = T)
#' 
#' paths <- unname(unlist(sapply(dirs, function(p) list.files(p, f = T, pattern = ".mzXML"))))
#' newPath <- tibble(path = paths)
#' 
#' newPath$basename <- basename(newPath$path)
#' 
#' allPaths <- merge(oldPath, newPath, by = "basename", suffixes = c("_old", "_new"), all.x = T)
#' 
#' for (i in seq_len(nrow(allPaths))) {
#'   message(paste("Verarbeite Datei", i, "von", nrow(allPaths), "..."))
#'   tryCatch(
#'     changeMsrawfilePath(
#'       rfIndex, 
#'       oldPath = allPaths$path_old[i], 
#'       newPath = allPaths$path_new[i], 
#'       checkType = "md5"
#'     ),
#'     error = function(cnd) {
#'       message("error in file ", allPaths$basename[i])
#'       message(cnd)
#'     }
#'   )
#' }
#' 
#' message("Pfadänderung erfolgreich für alle Dateien abgeschlossen!")
#' }
changeMsrawfilePath <- function(rfIndex, oldPath, newPath, checkType = "md5") {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(all(file.exists(oldPath, newPath)))
  stopifnot(checkType %in% c("md5", "filesize"))
  stopifnot(basename(oldPath) == basename(newPath))
  stopifnot(oldPath != newPath)
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
