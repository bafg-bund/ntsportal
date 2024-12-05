# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal



createNewIndexForResults <- function(escon, mappingType = "nts", indexName) {
  f <- switch(
    mappingType,
    nts = fs::path_package("ntsportal", "extdata", "nts_index_mappings.json"),
    dbas = fs::path_package("ntsportal", "extdata", "dbas_index_mappings.json"),
    spectral_library = fs::path_package("ntsportal", "extdata", "spectral_library_index_mappings.json"),
    stop("unknown mapping type")
  )
  mappings <- jsonlite::read_json(f)
  elastic::index_create(escon, indexName, body = mappings)
}


#' Change alias of an index to a new index.
#' 
#' @description Will delete the previous alias
#'
#' @param escon elastic connection object created by elastic::connect
#' @param indexName Name of index
#' @param aliasName Name of alias
#' @param closeAfter logical, should the previous index, to which the alias 
#' linked to prior to the change, be closed? 
#'
#' @return Passes on response from ES (acknowledged field, TRUE or FALSE), invisibly
es_move_alias <- function(escon, indexName, aliasName, closeAfter = FALSE) {
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
#' 
#' This is a secure method of changing the path of a doc in msrawfiles. 
#' Filename is not changed and must remain the same.
#'
#' @param escon ElasticSearch connection object created by `elastic::connect`
#' @param rfindex index name for rawfiles index
#' @param oldPath Current path in rawfiles index, file must exist (1 file)
#' @param newPath New path, file must exist (1 file)
#' @param checkType Method to check that files are the same, either "md5" (default) or "filesize"
#' 
#' @details
#' Both files must exist when making the change. The function takes some time 
#' if it compares md5-checksums of the two files. For large numbers of files use the
#' filesize check.
#' 
#' @return TRUE if change was successful (invisibly)
#' @export
#' 
#' @examples
#' \dontrun{
#' source("~/connect-ntsp.R")
#' rfindex <- "ntsp_index_msrawfiles_unit_tests"
#' res <- elastic::Search(escon, rfindex, source = "path")
#' opths <- sapply(res$hits$hits, function(x) x[["_source"]]$path)
#' fn <- basename(opths)
#' npths <- list.files("/scratch/nts/ntsportal_unit_tests/meas_files", f = T)
#' npths <- nps[sapply(fn, grep, x = opths)]
#' purrr::walk2(opths, npths, change_msrawfile_path, escon = escon, rfindex = rfindex)
#' }
change_msrawfile_path <- function(escon, rfindex, oldPath, newPath, checkType = "md5") {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(all(file.exists(oldPath, newPath)))
  stopifnot(checkType %in% c("md5", "filesize"))
  # Check that both files are the same
  # Same name
  stopifnot(basename(oldPath) == basename(newPath))
  # Same content (slow)
  if (checkType == "md5" && tools::md5sum(oldPath) != tools::md5sum(newPath))
    stop(oldPath, " and ", newPath, " are not the same")
  # Same size (faster)
  if (checkType == "filesize" && file.size(oldPath) != file.size(newPath)) {
    stop(oldPath, " and ", newPath, " are not the same")
  }
  
  res1 <- elastic::Search(escon, rfindex, body = sprintf('
    {
      "query": {
        "term": {
          "path": {
            "value": "%s"
          }
        }
      },
      "size": 1,
      "_source": false
    }
    ', normalizePath(oldPath))
  )
  if (res1$hits$total$value == 0) {
    warning(oldPath, " not found in ", rfindex, " index. No change made.")
  } else if (res1$hits$total$value > 1) {
    stop(oldPath, " found more than once in ", rfindex, " index. Please correct.")
  } else {
    docId <- as.character(res1$hits$hits[[1]]["_id"])
    res2 <- elastic::docs_update(escon, rfindex, id = docId, body = 
                                   sprintf('
      {
        "script": {
          "source": "ctx._source.path = params.newPath",
          "params": {
            "newPath": "%s"
          }
        }
      }
     ', normalizePath(newPath))                               
    )
    invisible(res2$result == "updated")
  }
}

#' Change filename in the msrawfiles-db and on the filesystem
#' 
#' This function will change both filename and path of the doc.
#' 
#' @param escon ElasticSearch connection object created by `elastic::connect`
#' @param rfindex index name for msrawfiles index
#' @param oldPath original path
#' @param newPath updated path
#'
#' @return True (invisibly) if successful
#' @export
#'
change_msrawfile_filename <- function(escon, rfindex, oldPath, newPath) {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(file.exists(oldPath))
  stopifnot(grepl("^ntsp_msrawfiles", rfindex))
  
  oldName <- basename(oldPath)
  newName <- basename(newPath)
  stopifnot(length(oldName) == 1, length(newName) == 1)
 
  res1 <- elastic::Search(escon, rfindex, body = sprintf('
    {
      "query": {
        "term": {
          "filename": {
            "value": "%s"
          }
        }
      },
      "size": 1,
      "_source": false
    }
    ', oldName)
  )
  if (res1$hits$total$value == 0) {
    warning(oldName, " not found in ", rfindex, " index. No change made.")
    return(FALSE)
  } else if (res1$hits$total$value > 1) {
    stop(oldName, " found more than once in ", rfindex, " index. Please correct.")
  } else {
    docId <- as.character(res1$hits$hits[[1]]["_id"])
    res2 <- elastic::docs_update(escon, rfindex, id = docId, body = 
      glue_json('
      {
        "script": {
          "source": "ctx._source.path = params.newPath; 
                     ctx._source.filename = params.newName",
          "params": {
            "newPath": "[[newPath]]",
            "newName": "[[newName]]"
          }
        }
      }
     ')                               
    )
    
    res2 <- elastic::docs_update(escon, rfindex, id = docId, body = ns)
    
    # Change filename on filesystem
    file.rename(from = oldPath, to = newPath)
  }
  logger::log_info("File {oldName} updated to {newName}")
  invisible(TRUE)
}


