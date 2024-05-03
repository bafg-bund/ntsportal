
# Rawfiles-Index management functions ####

#' Get current index name based on the alias
#' 
#' The current date will be used in the index name
#' 
#' @param aliasName 
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#'
#' @return
#'
dbas_index_from_alias <- function(aliasName, dateNum = NULL) {
  if (is.null(dateNum)) {
    dateNum <- format(Sys.Date(), "%y%m%d")
  } else {
    stopifnot(grepl("\\d{6}", dateNum))
  }
  sub("^(ntsp_)dbas_", paste0("\\1index_dbas_v", dateNum, "_"), aliasName)
}

#' Create a new, empty dbas index 
#' 
#' Will create a new index based on the alias with _v<date> appended after _dbas.
#' The new index name will be written to msrawfiles index.
#' 
#' @param escon elastic connection object created by elastic::connect
#' @param rfindex index name for rawfiles index
#' @param aliasName 
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#'
#' @return
#' @export
#'
create_dbas_index <- function(escon, rfindex, aliasName, dateNum = NULL) {
  
  # Name of index for this alias
  indNew <- dbas_index_from_alias(aliasName, dateNum)
  
  # Create new index
  resCrea <- put_dbas_index(escon, indNew)
  if (resCrea$acknowledged)
    logger::log_info("Acknowledged index creation {indNew}") else stop("Index creation {indNew} failed")
  
  # Add new index name to rfindex
  resNameChange <- elastic::docs_update_by_query(
    escon, rfindex, body = 
      sprintf('
      {
      "query": {
        "term": {
          "dbas_alias_name": {
            "value": "%s"
          }
        }
      },
      "script": {
        "source": "ctx._source.dbas_index_name = params.newName",
        "lang": "painless",
        "params": {
          "newName": "%s"
        }
      }
    }
    ', aliasName, indNew)
  )
  
  if (resCrea$acknowledged && length(resNameChange$failures) == 0) {
    logger::log_info("Added index name to msrawfiles")
  } else {
    stop("Adding index name {indNew} to msrawfiles failed")
  }
  
  invisible(indNew)
}

#' Create new index for all documents
#'
#' @param escon 
#' @param rfindex 
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#' 
#' @return
#' @export
#'
create_dbas_index_all <- function(escon, rfindex, dateNum = NULL) {
  # Get list of all aliases
  res <- elastic::Search(
    escon, rfindex, size = 0,
    body = list(
      aggs = list(
        aliases = list(
          terms = list(
            field = "dbas_alias_name",
            size = 100
          )
        )
      )
    )
  )
  a <- vapply(res$aggregations$aliases$buckets, "[[", i = "key", character(1))
  for (i in a) {
    create_dbas_index(escon, rfindex, aliasName = i, dateNum = dateNum)
  }
  invisible(TRUE)
}


#' Change alias of an index to a new index.
#' 
#' Will delete the previous alias
#'
#' @param escon elastic connection object created by elastic::connect
#' @param indexName 
#' @param aliasName
#' @param closeAfter logical, should the previous index, to which the alias 
#' linked to prior to the change, be closed? 
#'
#' @return
#' @export
#'
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

#' Update alias names
#'
#' This function will update all the aliases in msrawfiles to use the current
#' indices found in msrawfiles
#' 
#' @param escon 
#' @param rfindex 
#'
#' @return
#' @export
#'
#' @examples
update_alias_all <- function(escon, rfindex) {
  # Get all index names and alias names
  res <- elastic::Search(
    escon, rfindex, size = 0,
    body = list(
      aggs = list(
        aliases = list(
          terms = list(
            field = "dbas_alias_name",
            size = 100
          ),
          aggs = list(
            indices = list(
              terms = list(
                field = "dbas_index_name",
                size = 100
              )
            )
          )
        )
      )
    ) 
  )
  l <- lapply(res$aggregations$aliases$buckets, function(aliases) {
    if (length(aliases$indices$buckets) != 1) {
      stop("There is not a 1:1 relationship between aliases and indices")
    }
    data.frame(alias = aliases$key, index = aliases$indices$buckets[[1]]$key)
  })
  df <- do.call("rbind", l)
  
  for (i in 1:nrow(df)) {
    es_move_alias(escon, indexName = df[i, "index"], aliasName = df[i, "alias"],
                  closeAfter = TRUE)
  }
  
  invisible(TRUE)
}

#' Remove the old dbas index alias and create a new one
#' 
#' This is specific for dbas aliases because of the need for v4 alias names for 
#' Kibana (will be depricated in the future). For a general function to change
#' alias names, see es_move_alias. 
#' 
#' This function will also close the old index after creation of the new alias
#'
#' @param escon elastic connection object created by elastic::connect
#' @param indexName 
#' @param aliasName 
#'
#' @return
#'
move_dbas_alias <- function(escon, indexName, aliasName) {
  stop("deprecated 2024-01-02")
  
  #browser(expr = grepl("hessen", aliasName))
  # name of index currently at alias
  #aliasName2 <- sub("^(g2_dbas)", "\\1_v4", aliasName)
  
  
  aliases <- elastic::cat_aliases(escon, index = aliasName, parse = T)
  if (is.null(aliases)) {
    aliases <- data.frame(aliasName, indexName)
  } else {
    # delete previous alias
    previousIndex <- aliases[1, 2]
    res1 <- elastic::alias_delete(escon, index = aliases[1, 2], alias = aliases[1, 1]) 
  }
  
  # aliases2 <- elastic::cat_aliases(escon, index = aliasName2, parse = T)
  # if (is.null(aliases2)) {
  #   aliases2 <- data.frame(aliasName2, indexName)
  # } else {
  #   res2 <- elastic::alias_delete(escon, index = aliases2[1, 2], alias = aliases2[1, 1])  
  # }
  
  # create new alias
  res3 <- elastic::alias_create(escon, indexName, aliasName)
  logger::log_info("Created alias {aliasName} on index {indexName}")
  #res4 <- elastic::alias_create(escon, indexName, aliasName2)
  
  #ok <- all(vapply(list(res3, res4), "[[", i = "acknowledged", logical(1)))
  ok <- res3$acknowledged
  
  if (!ok) {
    logger::log_error("Error in creating alias {aliasName} on index {indexName}")
  }
  
  if (ok && exists("previousIndex") && length(previousIndex) == 1) {
    elastic::index_close(escon, previousIndex)
    logger::log_info("Closed index {previousIndex}")
  }
  
  ok
}

#' Change the path of one document in msrawfiles
#' 
#' This is a secure method of changing the path of a doc in msrawfiles. 
#' Filename is not changed and must remain the same.
#'
#' @param escon elastic connection object created by elastic::connect
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
#' 
#'
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
#' @param escon elastic connection object created by elastic::connect
#' @param rfindex index name for msrawfiles index
#' @param oldPath originally path
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



