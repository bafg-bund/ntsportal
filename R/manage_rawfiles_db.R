
# Rawfiles-Index management functions ####

#' Get current index name based on the alias
#' 
#' The current date will be used in the index name
#' 
#' @param aliasName 
#' @param dateNum 
#'
#' @return
#'
dbas_index_from_alias <- function(aliasName, dateNum = NULL) {
  if (is.null(dateNum)) {
    dateNum <- format(Sys.Date(), "%y%m%d")
  } else {
    stopifnot(grepl("\\d{6}", dateNum))
  }
  sub("^(g2_dbas_)", paste0("\\1v", dateNum, "_"), aliasName)
}

#' Create a new, empty dbas index 
#'
#' @param escon elastic connection object created by elastic::connect
#' @param aliasName 
#' @param rfIndex index name for rawfiles index
#'
#' @return
#' @export
#'
create_dbas_index <- function(escon, aliasName, rfIndex) {
  
  # Name of index for this alias
  indNew <- dbas_index_from_alias(aliasName)
  log_info("Creating new index {indNew}")
  
  resCrea <- elastic::index_create(
    escon, indNew, body = 
      '
    {
      "mappings" : {
        "dynamic": "strict",
        "properties" : {
          "area" : {"type" : "float"},
          "area_is" : {"type" : "float"},
          "area_normalized" : {"type" : "float"},
          "intensity" : {"type" : "float"},
          "intensity_is" : {"type" : "float"},
          "intensity_normalized" : {"type" : "float"},
          "cas" : {"type" : "keyword"},
          "comment" : {"type" : "text"},
          "comp_group" : {
            "type" : "keyword"
          },
          "conc" : {"type" : "float"},  
          "tag": {"type": "keyword"},
          "data_source" : {"type" : "keyword"},
          "start" : {
            "type" : "date",
            "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM-dd HH:mm"
          },
          "km": { "type" : "float" },
          "gkz" : { "type" : "integer" },
          "river" : { "type" : "keyword" },
          "duration" : {"type" : "float"},
          "date_import" : {
            "type" : "date",
            "format" : "epoch_second"
          },
          "eic" : {
            "type" : "nested",
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "time" : {
                "type" : "short"
              }
            }
          },
          "chrom_method" : {"type" : "keyword"},
          "loc" : {"type" : "geo_point"},
          "matrix" : {"type" : "keyword"},
          "ms1" : {                           
            "type" : "nested",                
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "mz" : {
                "type" : "float"
              }
            }
          },
          "ms2" : {
            "type" : "nested",
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "mz" : {
                "type" : "float"
              }
            }
          },
          "mz" : {"type" : "float"},
          "name" : {"type" : "keyword"},
          "norm_a" : {"type" : "float"},
          "pol" : {"type" : "keyword"},
          "rt" : {"type" : "float"},
          "rtt" : {
            "type" : "nested",
            "properties" : {
              "method" : {
                "type" : "keyword"
              },
              "predicted" : {
                "type" : "boolean"
              },
              "rt" : {
                "type" : "float"
              }
            }
          },
          "filename" : {"type" : "keyword"},
          "station" : {"type" : "keyword"},
          "inchikey" : {"type" : "keyword"},
          "inchi" : {"type" : "keyword"},
          "smiles" : {"type" : "keyword"},
          "mw" : {"type" : "float"},
          "adduct" : {"type" : "keyword"},
          "formula" : {"type" : "keyword"}
        }
      }
    }
    ')
  if (resCrea$acknowledged)
    log_info("Acknowledged index creation") else stop("Index creation failed")
  
  # Add new index name to rfIndex
  resNameChange <- elastic::docs_update_by_query(
    escon, rfIndex, body = 
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
  
  if (length(resNameChange$failures) == 0)
    log_info("Successful index name change") else stop("Unable to change index name")
  
  invisible(indNew)
}


#' Change alias of an index to a new index.
#' 
#' Will delete the previous alias
#'
#' @param escon elastic connection object created by elastic::connect
#' @param indexName 
#' @param aliasName 
#'
#' @return
#' @export
#'
es_move_alias <- function(escon, indexName, aliasName) {
  aliases <- elastic::cat_aliases(escon, index = aliasName, parse = T)
  # Delete previous alias
  if (!is.null(aliases)) {
    res1 <- elastic::alias_delete(escon, index = aliases[1, 2], alias = aliases[1, 1]) 
  }
  res <- elastic::alias_create(escon, indexName, aliasName)
  res$acknowledged
}

#' Remove the old dbas index alias and create a new one
#' 
#' This is specific for dbas aliases because of the need for v4 alias names for 
#' Kibana (will be depricated in the future). For a general function to change
#' alias names, see es_move_alias
#'
#' @param escon elastic connection object created by elastic::connect
#' @param indexName 
#' @param aliasName 
#'
#' @return
#' @export
#'
move_dbas_alias <- function(escon, indexName, aliasName) {
  #browser(expr = grepl("hessen", aliasName))
  # name of index currently at alias
  
  aliasName2 <- sub("^(g2_dbas)", "\\1_v4", aliasName)
  
  aliases <- elastic::cat_aliases(escon, index = aliasName, parse = T)
  if (is.null(aliases)) {
    aliases <- data.frame(aliasName, indexName)
  } else {
    # delete previous alias
    res1 <- elastic::alias_delete(escon, index = aliases[1, 2], alias = aliases[1, 1]) 
  }
  
  aliases2 <- elastic::cat_aliases(escon, index = aliasName2, parse = T)
  if (is.null(aliases2)) {
    aliases2 <- data.frame(aliasName2, indexName)
  } else {
    res2 <- elastic::alias_delete(escon, index = aliases2[1, 2], alias = aliases2[1, 1])  
  }
  
  # create new alias
  res3 <- elastic::alias_create(escon, indexName, aliasName)
  res4 <- elastic::alias_create(escon, indexName, aliasName2)
  all(vapply(list(res3, res4), "[[", i = "acknowledged", logical(1)))
}

#' Change the path of one document in a rawfiles db
#'
#' @param escon elastic connection object created by elastic::connect
#' @param rfindex index name for rawfiles index
#' @param oldPath Current path in rawfiles index, file must exist
#' @param newPath New path, file must exist
#'
#' @details
#' Both files must exist when making the change. The function takes some time 
#' because it compares md5-checksums of the two files.
#' 
#' @return TRUE if change was successful (invisibly)
#' @export
#'
change_msrawfile_path <- function(escon, rfindex, oldPath, newPath) {
  stopifnot(length(oldPath) == 1, length(newPath) == 1)
  stopifnot(all(file.exists(oldPath, newPath)))
  # check that both files are the same
  stopifnot(basename(oldPath) == basename(newPath))
  if (tools::md5sum(oldPath) != tools::md5sum(newPath))
    stop(oldPath, " and ", newPath, " are not the same")
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
          "source": "ctx._source.path = \'%s\'",
          "lang": "painless"
        }
      }
     ', normalizePath(newPath))                               
    )
    invisible(res2$result == "updated")
  }
}