# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.

# Utility functions ####


#' Return one non-aligned feature (without a ufid)
#' 
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param index Elasticsearch index name
#'
#' @return returns a ntsportal::feature object
#'
#' @export
feature_no_ufid <- function(escon, index) {

  res <- elastic::Search(escon, index, body = '
        {
          "query": {
            "bool": {
              "must_not": [
                {
                  "exists": {
                    "field": "ufid"
                  }
                }
              ]
            }
          },
          "size": 1
        }
  ')
  if (length(res$hits$hits) == 0)
    return(NULL)

  doc <- res$hits$hits[[1]]$`_source`
  new_feature(mz = doc$mz, rt = doc$rt, chrom_method = doc$chrom_method,
              rtt = do.call("rbind", lapply(doc$rtt, as.data.frame)),
              ms1 = data.frame(mz = sapply(doc$ms1, function(x) x$mz), int = sapply(doc$ms1, function(x) x$int)),
              ms2 = data.frame(mz = sapply(doc$ms2, function(x) x$mz), int = sapply(doc$ms2, function(x) x$int)),
              pol = doc$pol, es_id = res$hits$hits[[1]]$`_id`)
}

#' Return a feature, annotated with Carbamazepine, without a ufid
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#'
#' @return returns a ntsportal::feature object
#' @export
#'
feature_no_ufid_cbz <- function(escon, index) {

  res <- elastic::Search(escon, index, body = '
                         {
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "ufid"
          }
        }
      ],
      "must": [
        {
          "term": {
            "name": {
              "value": "Carbamazepine"
            }
          }
        },
        {
          "term": {
            "chrom_method": {
              "value": "bfg_nts_rp1"
            }
          }
        }
      ]
    }
  },
  "size": 1
}
  ')

  if (length(res$hits$hits) == 0)
    return("no more hits")
  doc <- res$hits$hits[[1]]$`_source`
  new_feature(mz = doc$mz, rt = doc$rt,
              rtt = do.call("rbind", lapply(doc$rtt, as.data.frame)),
              ms1 = data.frame(mz = sapply(doc$ms1, function(x) x$mz), int = sapply(doc$ms1, function(x) x$int)),
              ms2 = data.frame(mz = sapply(doc$ms2, function(x) x$mz), int = sapply(doc$ms2, function(x) x$int)),
              pol = doc$pol, es_id = res$hits$hits[[1]]$`_id`)
}


#' Get non-aligned features from a particular compound and polarity
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param compName length 1 character
#' @param polarity length 1 character ("pos", "neg")
#'
#' @return character vector NTSP IDs for features (max. 10000)
#' @export
#'
es_ids_to_assign_name <- function(escon, index, compName, polarity) {
  res <- elastic::Search(escon, index, body = sprintf('
  {
    "query": {
      "bool": {
        "must_not": [
          {
            "exists": {
              "field": "ufid"
            }
          }
        ],
        "filter": [
          {
            "term": {
              "name": "%s"
            }
          },
          {
            "term": {
              "pol": "%s"
            }
          }
        ]
      }
    },
    "_source": false,
    "size": 10000
  }
  ', compName, polarity))
  message("Total found: ", res$hits$total$value)
  sapply(res$hits$hits, function(x) x[["_id"]])
}

#' Get feature (doc) from ntsp with a document-ID
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param id Elasticsearch document ID
#'
#' @return ntsportal::feature object
#' 
#' @export
#'
es_feat_from_id <- function(escon, index, id) { # id <- "2vLQwXsB5nUKfQcuMOAv"
  res <- elastic::docs_get(escon, index, id)
  doc <- res[["_source"]]
  stopifnot(all(c("mz", "rt", "pol", "chrom_method") %in% names(doc)))

  newFeat <- new_feature(mz = doc$mz, rt = doc$rt,
              pol = doc$pol, chrom_method = doc$chrom_method, es_id = id)

  if ("rtt" %in% names(doc))
    newFeat$rtt <- do.call("rbind", lapply(doc$rtt, as.data.frame))

  if ("ms1" %in% names(doc))
    newFeat$ms1 <- data.frame(mz = sapply(doc$ms1, function(x) x$mz),
                              int = sapply(doc$ms1, function(x) x$int))

  if ("ms2" %in% names(doc))
    newFeat$ms2 <- data.frame(mz = sapply(doc$ms2, function(x) x$mz),
                              int = sapply(doc$ms2, function(x) x$int))
  
  if ("rt_clustering" %in% names(doc))
    newFeat$rt_clustering <- doc$rt_clustering

  newFeat
}

#' Get list of features from ntsp with document-IDs
#' 
#' Can one or more IDs. Index patterns including comma-sep are not allowed for index name.
#' 
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param esids character vector of IDs
#'
#' @return list of Feature objects
#' @export
#'
es_feat_from_ids <- function(escon, index, esids) { 
  
  if (length(esids) == 1) {
    res <- elastic::docs_get(escon, index, esids, verbose = FALSE)
    docs <- list(res[["_source"]])
  } else {
    res <- elastic::docs_mget(escon, index, ids = esids, verbose = FALSE)
    docs <- lapply(res$docs, "[[", i = "_source")
  }
  
  newFeats <- Map(function(doc, esid) {
    newFeat <- new_feature(mz = doc$mz, rt = doc$rt,
                            pol = doc$pol, chrom_method = doc$chrom_method, 
                            es_id = esid)
    
    if ("rtt" %in% names(doc))
      newFeat$rtt <- do.call("rbind", lapply(doc$rtt, as.data.frame))
    
    if ("ms1" %in% names(doc))
      newFeat$ms1 <- data.frame(mz = sapply(doc$ms1, function(x) x$mz),
                                int = sapply(doc$ms1, function(x) x$int))
    
    if ("ms2" %in% names(doc))
      newFeat$ms2 <- data.frame(mz = sapply(doc$ms2, function(x) x$mz),
                                int = sapply(doc$ms2, function(x) x$int))
    
    if ("rt_clustering" %in% names(doc))
      newFeat$rt_clustering <- doc$rt_clustering
    
    newFeat
  }, docs, esids)
  names(newFeats) <- esids
  newFeats
}



#' Check feature
#'
#' @description Checks that feature has mz.
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param id character ID length 1
#'
#' @return TRUE if feature ok
#' @export
#'
es_check_feat <- function(escon, index, id) {
  res <- elastic::Search(escon, index, body = sprintf('
                                                      {
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "_id": "%s"
          }
        }
      ]
    }
  },
  "_source": "mz"
}
', id))
  mztest <- res$hits$hits[[1]][["_source"]]$mz
  is.numeric(mztest) && length(mztest) == 1
}

#' Update doc in ntsportal with ufid
#'
#' all the information needed is
#' contained within the feature object
#' 
#' @param feat ntsportal::feature object
#' @param escon elasticsearch connection object created by elastic::connect
#' @param index elasticsearch index name
#'
#' @return TRUE if update successful
#' @export
es_add_ufid <- function(feat, escon, index) {
  feat <- validate_feature(feat)
  stopifnot("ufid" %in% names(feat))

  res <- elastic::docs_update(escon, index, id = feat$es_id, refresh = "wait_for",
                              body = sprintf(
                                '
    {
      "script": {
        "source": "ctx._source.ufid = params.thisUfid",
        "params": {
          "thisUfid": %i
        }
      }
      
    }
    ', feat$ufid)
  )

  if (res$result == "updated")
    TRUE else FALSE
}

#' Delete all ufid entries for a ufid
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param index Elasticsearch index name
#' @param ufidToDelete ID of ufid to delete
#'
#' @return TRUE invisibly
#' @export
#'
es_remove_ufid <- function(escon, index, ufidToDelete) {
  rese <- elastic::docs_update_by_query(escon, index, body = sprintf('
     {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ufid\')",
        "lang": "painless"
      }
    }
  ', ufidToDelete)) 
  log_info("Successful removal of ufid {ufidToDelete}")
  invisible(TRUE)
}

#' Add ufid entry to a list of features in elasticsearch
#'
#' This function often leads to errors
#' 
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param ufid_to_add integer length 1 ufid to add
#' @param ids_for_update character of document IDs, max. 65536
#' @param ufidType either ufid or ufid2
#'
#' @return TRUE if update successful
#' @export
es_add_ufid_to_ids <- function(escon, index, ufid_to_add, ids_for_update,
                               ufidType = "ufid", verbose = FALSE) {
  if (length(ids_for_update) > 65536)
    stop("exceeded maximum allowed ids for ufid ", ufid_to_add)
  
  stopifnot(ufidType %in% c("ufid", "ufid2"))
  id_search_string <- paste(shQuote(ids_for_update, type = "cmd"), collapse = ", ")
  
  res_update <- elastic::docs_update_by_query(
    escon, index, 
    refresh = "true", 
    body = sprintf('
      {
        "query": {
          "ids": {
            "values": [%s]
          }
        },
        "script": {
          "source" : "ctx._source.%s = params.thisUfid",
          "params" : {
            "thisUfid" : %i
          }
        }
      }
      ', 
      id_search_string, 
      ufidType,
      ufid_to_add
    )
  )
  
  if (is.null(res_update))
    stop("Ufid update failed for ufid ", ufid_to_add)
  if (length(ids_for_update) == res_update$updated) {
    if (verbose)
      logger::log_info("Successful ntsp update of ufid {ufid_to_add}")
  } else {
    stop("Ufid update failed for ufid ", ufid_to_add)
  }
    
  invisible(TRUE)
}

#' Add ufid2 entry to a list of features in elasticsearch
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param ufid2_to_add integer length 1 ufid2 to add
#' @param ids_for_update character of document IDs, max. 65536
#'
#' @return TRUE if successful
#' @export
es_add_ufid2_to_ids <- function(escon, index, ufid2_to_add, ids_for_update) {
  if (length(ids_for_update) > 65536)
    stop("exceeded maximum allowed ids")

  id_search_string <- paste(shQuote(ids_for_update, type = "cmd"), collapse = ", ")
  res_update <- elastic::docs_update_by_query(escon, index, refresh = "true", body = sprintf('
      {
        "query": {
          "ids": {
            "values": [%s]
          }
        },
        "script": {
          "source" : "ctx._source.ufid2 = params.thisUfid2",
          "params" : {
            "thisUfid2" : %i
          }
        }
      }
      ', id_search_string, ufid2_to_add))
  total_updated <- res_update$updated
  if (length(ids_for_update) == total_updated)
    logger::log_info("successful ntsp update") else stop("update esdb not complete")
  TRUE
}


#' Reformat results from elastic into a list of features
#
#' Given a result list from elastic::Search, reformat this into a list of
#' features of the feature class.
#'
#' @param res response list from elastic::Search
#' @param fields which fields should be included in the features (at least mz,
#' rt, pol and chrom_method must be included)
#'
#' @return list of feature objects
#' @export
es_res_feat_list <- function(res, fields = c("mz", "pol", "rt", "chrom_method")) {
  stopifnot(all(c("mz", "pol", "rt", "chrom_method") %in% fields))
  stopifnot(length(res) == 4)
  stopifnot(names(res) == c("took", "timed_out", "_shards", "hits"))

  resBuckets <- res$hits$hits
  stopifnot(length(resBuckets) > 0)

  ftsl <- lapply(resBuckets, function(rft) {
    s <- rft$`_source`

    nft <- ntsportal::new_feature(
      mz = s$mz, rt = s$rt, pol = s$pol, es_id = rft$`_id`,
      chrom_method = s$chrom_method
    )

    if ("filename" %in% fields)
      nft$filename <- s$filename
    if ("intensity" %in% fields)
      nft$intensity <- s$intensity
    if ("date_import" %in% fields)
      nft$date_import <- s$date_import
    if ("data_source" %in% fields)
      nft$data_source <- s$data_source
    if ("eic" %in% fields) {
      eic_temp <- as.data.frame(do.call("rbind", lapply(s$eic, unlist)))
      nft$eic <- eic_temp
    }

    nft
  })
  names(ftsl) <- vapply(ftsl, "[[", character(1), i = "es_id")
  ftsl
}



#' Assign ufids to features without MS2 (gap-filling)
#'
#' For a given ufid, will look for features of the same batch with similar
#' mz, rt and eic (chrom. peak shape) and assign these to the same ufid.
#'
#' Candidate features (based on mz and rt) are joined to already assigned features
#' and all features are clustered based on dtw correlation of EICs followed by DBSCAN clustering.
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param ufid_to_fill
#' @param min_number minimum number of MS2 ufids needed in the cluster to assign
#' ufid to other features in that cluster
#' @param mztol_gap_fill_mda
#' @param rttol_gap_fill_min
#'
#' @return Integer. Number of features assigned (0 for no assignment)
#' @export
es_ufid_gap_fill <- function(escon, index, ufid_to_fill, min_number = 2,
                              mztol_gap_fill_mda = 5,
                              rttol_gap_fill_min = 0.3) {
  # ufid_to_fill <- 908
  # min_number = 2
  # mztol_gap_fill_mda = 5
  # rttol_gap_fill_min = 0.3
  #browser()
  #logger::log_info("Gap-filling on ufid {ufid_to_fill}")
  
  # get info of current features in index
  fieldsVec <- c("mz", "rt","filename", "chrom_method", "data_source",
                 "date_import", "eic", "pol", "intensity")
  res1 <- elastic::Search(escon, index, body = sprintf('
                                               {
  "query": {
    "bool": {
      "filter": [
        {
          "nested": {
            "path": "ms2",
            "query": {
              "exists": {
                "field": "ms2.mz"
              }
            }
          }
        },
        {
          "nested": {
            "path": "eic",
            "query": {
              "exists": {
                "field": "eic.time"
              }
            }
          }
        },
        {
          "term": {
            "ufid": %i
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "source": {
      "terms": {
        "field": "data_source"
      },
      "aggs": {
        "method": {
          "terms": {
            "field": "chrom_method"
          },
          "aggs": {
            "dateImport": {
              "date_histogram": {
                "field": "date_import",
                "fixed_interval": "1s",
                "min_doc_count": %i
              },
              "aggs": {
                "ids": {
                  "top_hits": {
                    "size": 100,
                    "_source": false
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}                                             
', ufid_to_fill, min_number))
  
  
  bIds <- lapply(res1$aggregations$source$buckets, function(x) {
    lapply(x$method$buckets, function(y) {
      lapply(y$dateImport$buckets, function(z) {
        vapply(z$ids$hits$hits, "[[", i = "_id", character(1)) 
      })
    })
  })
  bIds <- unlist(unlist(bIds, F), F)
  batches <- lapply(bIds, function(x) {
    idString <- paste(shQuote(x, type = "cmd"), collapse = ", ")
    resx <- elastic::Search(
      escon, index,
      body = sprintf('
        {
          "query": {
            "ids": {
              "values": [%s]
            }
          }
        }             
                     ', idString)
    )
    ntsportal::es_res_feat_list(resx, fieldsVec)
  })
  
  logger::log_info("{length(batches)} batches found for gap-filling.")
  
  # Per batch, add ids of features to update with this ufid:
  ids_to_update <- character(0)
  for (ftsb in batches) {  # ftsb <- batches[[1]]
    
    # Find other features of the same batch without MS2 and without ufid
    mthd <- ftsb[[1]]$chrom_method
    ds <- ftsb[[1]]$data_source
    dimp <- as.integer(median(vapply(ftsb, "[[", integer(1), i = "date_import")))
    ave_mz <- mean(vapply(ftsb, "[[", numeric(1), i = "mz"))
    ave_rt <- mean(vapply(ftsb, "[[", numeric(1), i = "rt"))
    pol <- ftsb[[1]]$pol
    
    res3 <- elastic::Search(
      escon,
      index,
      source = fieldsVec,
      size = 10000,
      body = sprintf(
        '
          {
            "query": {
              "bool": {
                "must_not": [
                  {
                    "nested": {
                      "path": "ms2",
                      "query": {
                        "exists": {
                          "field": "ms2.mz"
                        }
                      }
                    }
                  },
                  {
                    "exists": {
                      "field": "ufid"
                    }
                  }
                ],
                "must": [
                  {
                    "nested": {
                      "path": "eic",
                      "query": {
                        "exists": {
                          "field": "eic.time"
                        }
                      }
                    }
                  },
                  {
                    "range": {
                      "mz": {
                        "gte": %.4f,
                        "lte": %.4f
                      }
                    }
                  },
                  {
                    "range": {
                      "rt": {
                        "gte": %.2f,
                        "lte": %.2f
                      }
                    }
                  },
                  {
                    "term": {
                      "chrom_method": "%s"
                    }
                  },
                  {
                    "term": {
                      "data_source": "%s"
                    }
                  },
                  {
                    "range": {
                      "date_import": {
                        "gte": %i,
                        "lte": %i,
                        "format": "epoch_second"
                      }
                    }
                  },
                  {
                    "term": {
                      "pol": "%s"
                    }
                  }
                ]
              }
            },
            "_source": ["filename", "eic.int", "eic.time"]
          }
        ',
        ave_mz - mztol_gap_fill_mda / 1000,
        ave_mz + mztol_gap_fill_mda / 1000,
        ave_rt - rttol_gap_fill_min,
        ave_rt + rttol_gap_fill_min,
        mthd,
        ds,
        dimp,
        dimp,
        pol
      )
    )
    
    # If nothing found, no features to update, move to next batch
    numCand <- res3$hits$total$value
    if (numCand == 0)
      next
    #browser()
    ftCand <- ntsportal::es_res_feat_list(res3, fieldsVec)
    
    # Each candidate must be from a file which does not already contain the
    # ufid to fill
    not_already_added2 <- function(fnToCheck, uToCheck) {
      res2 <- elastic::Search(
        escon, index, source = FALSE,
        body = sprintf(
          '
            {
              "query": {
                "bool": {
                  "filter": [
                    {
                      "term": {
                        "ufid": %i
                      }
                    },
                    {
                      "term": {
                        "filename": "%s"
                      }
                    }
                  ]
                }
              }
            }
          ',
          uToCheck,
          fnToCheck)
      )
      return(res2$hits$total$value == 0)
    }
    flnCand <- vapply(ftCand, "[[", character(1), i = "filename")
    keep <- vapply(flnCand, not_already_added2, logical(1), uToCheck = ufid_to_fill)
    if (all(!keep))
      next
    ftCand <- ftCand[keep]
    
    # Compute eic correlation for all features with eachother
    
    eicProt <- lapply(ftsb, "[[", i = "eic")
    names(eicProt) <- names(ftsb)
    eicCand <- lapply(ftCand, "[[", i = "eic")
    names(eicCand) <- names(ftCand)
    eics <- c(eicProt, eicCand)
    
    # Round time to nearest second, average the intensity for duplicate times
    eics <- lapply(eics, function(x) {
      x$time <- round(x$time)
      dupTimes <- unique(x$time[duplicated(x$time)])
      for (dt in dupTimes)
        x[which(x$time == dt)[1], "int"] <- mean(x[x$time == dt, "int"])
      x[!duplicated(x$time), ]
    })
    
    eicm <- suppressWarnings(Reduce(function(a, b) merge(a, b, by = "time", all = TRUE), eics))
    # pardist does not work with NAs, need to set them to 0
    eicm[is.na(eicm)] <- 0
    eicmt <- t(eicm[, -1])
    colnames(eicmt) <- eicm[, 1]
    rownames(eicmt) <- names(eics)
    
    # z-normalize
    znorm <- function(ts) {
      ts.mean <- mean(ts)
      ts.dev <- sd(ts)
      (ts - ts.mean)/ts.dev
    }
    eicmtn <- t(apply(eicmt, 1, znorm))
    
    #rowMax <- apply(eicmt, 1, max, na.rm = TRUE)
    #eicmtn <- sweep(eicmt, 1, rowMax, "/")
    # plot(eicmtn[15, ])
    
    
    # dbscan clustering of dtw correlation
    distM <- parallelDist::parDist(eicmtn, method = "dtw", threads = config::get("cores"))
    # used elbow method on CBZ data to determine dbscan epsilon of 12
    # based on the soltalol data from lanuv increase epsilon to 24
    #dbscan::kNNdistplot(distM, 3)
    clust <- dbscan::dbscan(distM, 24, minPts = min_number)
    
    # those unassigned features within group of assigned feature are ear-marked for assignment
    cdf <- data.frame(
      es_id = names(eics),
      cl = clust$cluster,
      assigned = names(eics) %in% names(ftsb)
    )
    new_ids <- character(0)
    for (i in seq_len(nrow(cdf))) {
      if (cdf[i, "assigned"])
        next
      cli <- cdf[i, "cl"]
      if (cli == 0)
        next
      if (sum(cdf[cdf$cl == cli, "assigned"]) >= min_number)
        new_ids <- append(new_ids, cdf[i, "es_id"])
    }
    
    if (length(new_ids) == 0)
      next
    
    # check that all earmarked features are from different files,
    # in case multiple assignments, take most intense feature
    flnCand2 <- vapply(ftCand[new_ids], "[[", character(1), i = "filename")
    if (any(duplicated(flnCand2))) {
      flnDup <- unique(flnCand2[duplicated(flnCand2)])
      for (fln in flnDup) {  # fln <- flnDup[1]
        # get ids from which to choose
        idsDup <- names(flnCand2[flnCand2 == fln])
        # get intensity for each id
        intens <- vapply(ftCand[idsDup], "[[", numeric(1), i = "intensity")
        # keep only most intensive
        idsToRemove <- idsDup[-which.max(intens)]
        new_ids <- new_ids[!is.element(new_ids, idsToRemove)]
      }
    }
    
    ids_to_update <- append(ids_to_update, new_ids)
    
  }
  
  log_info("Found {length(ids_to_update)} features to update for ufid {ufid_to_fill} (gap-fill).")
  
  if (length(ids_to_update) == 0)
    return(0L)
  
  # Update es-database
  worked <- try(ntsportal::es_add_ufid_to_ids(escon, index, ufid_to_fill, ids_to_update))
  
  if (worked)
    message("Gap-filling complete") else return(NULL)
  
  invisible(length(ids_to_update))
}


# Main function - ufid assignment ####

#' Assign existing ufids to features if match is found
#' 
#' Go sequentially through ufid-db and assign ufids in es-db
#'
#' updates ufid-db as well when it is done. This can either be done for all
#' ufids, or for a selection by giving a vector of integers. 
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param udb Ufid library connection object created by DBI::dbConnect
#' @param index Elasticsearch index name
#' @param polarity
#' @param ufids if ufids is NULL (default), then all ufids will be processed
#' @param chromMethod name of chromatographic method to use, at the moment only "bfg_nts_rp1" possible
#'
#' @return TRUE if the function runs until the end (which does not mean that everything worked)
#' @export
#'
#' @import dplyr
es_assign_ufids <- function(
  escon,
  udb,
  index,
  polarity,
  ufids = NULL,
  chromMethod = "bfg_nts_rp1") {
  # library(dplyr)
  # polarity <- "pos"
  # ufids <- 8429L
  
  # At the moment the Ufid-db is small enough to fit in memory, so load it in, 
  # everything is much faster that way.
  ftt <- tbl(udb, "feature") %>% collect()
  rtt <- tbl(udb, "retention_time") %>% filter(method == chromMethod) %>% collect()
  ms2t <- tbl(udb, "ms2") %>% collect()

  mztol_rough <- config::get("mztol_rough_mda") / 1000
  rttol_rough <- config::get("rttol_rough_min")
  stopifnot(is.numeric(mztol_rough), mztol_rough > 0)

  if (!is.null(ufids) && is.numeric(ufids)) {
    all_ufids <- as.integer(ufids)
  } else {
    all_ufids <- ftt %>% select(ufid) %>% unname() %>% unlist()
  }

  for (uf in all_ufids) {  # uf <- 861L, uf <- all_ufids
    pol_ <- ftt %>% filter(ufid == !!unname(uf)) %>% select(polarity) %>% unlist()

    if (pol_ != polarity)
      next

    logger::log_info("Starting ufid {uf}")
    # collect candidate ids by rough filtering by mz-rt

    # MS2 matching ####

    mz_ <- ftt %>% filter(ufid == !!unname(uf)) %>% select(mz) %>% unlist()
    rt_ <- rtt %>% filter(ufid == !!unname(uf) & method == "bfg_nts_rp1") %>%
      select(rt) %>% unlist()
    
    res <- elastic::Search(escon, index, body = sprintf('
        {
          "query": {
            "bool": {
              "filter": [
                {
                  "nested": {
                    "path": "ms2",
                    "query": {
                      "exists": {
                        "field": "ms2.mz"
                      }
                    }
                  }
                },
                {
                  "term": {
                    "pol": "%s"
                  }
                },
                {
                  "range": {
                    "mz": {
                      "gte": %.4f,
                      "lte": %.4f
                    }
                  }
                },
                {
                  "nested": {
                    "path": "rtt",
                    "query": {
                      "bool": {
                        "must": [
                          {
                            "range": {
                              "rtt.rt": {
                                "gte": %.2f,
                                "lte": %.2f
                              }
                            }
                          },
                          {
                            "term": {
                              "rtt.method": {
                                "value": "%s"
                              }
                            }
                          }
                        ]
                      }
                    }
                  }
                }
              ],
              "must_not": [
                {
                  "exists": {
                    "field": "ufid"
                  }
                }
              ]
            }
          },
           "_source": "_id",
           "size": 10000
        }
        ', polarity,
           mz_ - mztol_rough,
           mz_ + mztol_rough,
           rt_ - rttol_rough,
           rt_ + rttol_rough,
           chromMethod))

    ids <- vapply(res$hits$hits, function(x) x[["_id"]], character(1))
    
    # nothing found, next ufid
    if (length(ids) == 0)
      next

    # collect features and then do udb_feature_match on these
    logger::log_info("Collecting {length(ids)} features from ntsp")
    fts <- tryCatch(
      suppressMessages(lapply(ids, ntsportal::es_feat_from_id, escon = escon, index = index)),
      error = function(e) {
        message("Error in ufid ", uf, ". error text: ", e, "\nskipping to next ufid")
      }
    )
    if (inherits(fts, "error") || is.null(fts))
      next
    
    logger::log_info("Matching features")
    
    matched_ufids <- tryCatch(
      vapply(
        fts,
        ntsportal::udb_feature_to_ufid_match,
        logical(1),
        ftt = ftt,
        rtt = rtt,
        ms2t = ms2t,
        ufid_to_match = uf,
        mztol = config::get("mztol_mda") / 1000,
        rttol = config::get("rttol_min"),
        ms2dpThresh = config::get("ms2_ndp_min_score"),
        ndp_m = config::get("ms2_ndp_m"),
        ndp_n = config::get("ms2_ndp_n"),
        mztolms2 = config::get("mztol_ms2_ndp_mda") / 1000,
        chromMethod = chromMethod
      ),
      error = function(e) {
        message("Error in ufid ", uf, ". error text: ", e, "\nskipping to next ufid")
      }
    )
    if (inherits(matched_ufids, "error") || is.null(matched_ufids) || all(!matched_ufids)) {
      logger::log_info("Nothing found, moving on")
      next
    }

    # keep matching fts
    fts <- fts[matched_ufids]
    # add ufid to fts
    fts <- lapply(fts, function(f) {f$ufid <- uf; f})



    # for each feature you want to assign a new ufid,
    # check that this ufid hasn't already been added to the file
    not_already_added <- function(f) {
      # get filename and import date of ft
      rs <- elastic::docs_get(escon, index, f$es_id, source = c("filename", "date_import"), 
                              verbose = FALSE)
      fn <- rs$`_source`$filename
      dateImport <- rs$`_source`$date_import

      # does this file already contain the ufid in question
      # we use date_import (2 day range) to filter out in case there are
      # other files with the same name added on other days
      rs2 <- elastic::Search(escon, index, body = sprintf('
      {
        "query": {
          "bool": {
            "filter": [
              {
                "term": {
                  "filename": "%s"
                }
              },
              {
                "range": {
                  "date_import": {
                    "gte": %i,
                    "lte": %i
                  }
                }
              },
              {
                "term": {
                  "ufid": %i
                }
              }
            ]
          }
        }
      }
      ', fn, dateImport - 86400, dateImport + 86400, f$ufid))
      rs2 <- rs2$hits$total$value
      return(rs2 == 0)
    }
    # only keep fts which have not already been added to the same file
    safe <- vapply(fts, not_already_added, logical(1))
    fts <- fts[safe]

    # make sure that for remaining fts, they all are from different files
    # if file appears more than once, keep the feature with the highest intensity
    if (length(fts) > 1) {
      finfo <- elastic::docs_mget(
        escon, index, ids = vapply(fts, "[[", character(1), "es_id"),
        source = c("filename", "date_import"), verbose = FALSE
      )
      finfo <- finfo$docs
      fls <- vapply(finfo, function(x) x$`_source`$filename, character(1))
      dts <- vapply(finfo, function(x) x$`_source`$date_import, integer(1))
      dts <- lubridate::as_date(lubridate::as_datetime(dts, tz = "Europe/Berlin"))  # what if the import was around midnight? maybe change to month
      nms <- paste(fls, dts, sep = "_")

      # in each sample, if more than one feature found, take the one with the highest intensity
      if (any(duplicated(nms))) {
        groupedBySample <- split(fts, nms)
        groupedBySampleHighest <- lapply(groupedBySample, function(ftgroup) {
          if (length(ftgroup) == 1)
            return(ftgroup[[1]])
          intens <- elastic::docs_mget(
            escon,
            index,
            ids = vapply(ftgroup, "[[", character(1), "es_id"),
            source = "intensity",
            verbose = F
          )
          intens <- intens$docs
          intens <- vapply(intens, function(x) x$`_source`$intensity, numeric(1))
          ftgroup[[which.max(intens)]]
        })
        fts <- groupedBySampleHighest
      }
    }

    if (length(fts) == 0) {
      message("Nothing found, moving on")
      next
    }

    logger::log_info("adding ufid {uf} to esdb")
    logger::log_info("total to add: {length(fts)}")
    ids_for_update <- vapply(fts, function(f) f$es_id, character(1))

    ntsportal::es_add_ufid_to_ids(escon, index, uf, ids_for_update)

    logger::log_info("updating ufid-db with new docs of ufid {uf}, using all available indexes")
    # Use all indexes for update, otherwise averaged spectra are only from
    # last updated index.
    
    ntsportal::udb_update(udb, escon, index = config::get("index_pattern"), uf)

    # run gap-filling for this ufid ####
    logger::log_info("Gap-filling for ufid {uf}")

    tryCatch(
      ntsportal::es_ufid_gap_fill(
        escon, index, uf, min_number = config::get("min_number_gap_fill"),
        mztol_gap_fill_mda = config::get("mztol_gap_fill_mda"),
        rttol_gap_fill_min = config::get("rttol_gap_fill_min")
      ),
      error = function(cnd) {
        logger::log_error("Gap-filling failed for ufid {uf}")
        logger::log_info("{conditionMessage(cnd)}")
      }
    )
   
    logger::log_info("Completed screening for ufid {uf}")
  }
  logger::log_info("Screening and assigning ufids complete for all \
                   requested ufids in es_assign_ufids")
  invisible(TRUE)
}

#' Add a tag to selected docs
#' 
#' Docs are selected by passing a vector of IDs 
#'
#' @param escon Elasticsearch connection object created by elastic::connect
#' @param index Elasticsearch index name
#' @param ids character of document IDs, max. 65536
#' @param tagToAdd your comment in the field tag ("yourtexthere")
#'
#' @return Integer. Number of docs updated (0 for no assignment)
#' @export
#'
#' @examples
es_add_tag_to_ids <- function(escon, index, ids, tagToAdd) {
  id_string <-  paste(shQuote(ids, type = "cmd"), collapse = ", ")
  res_update <- elastic::docs_update_by_query(
    escon, index, 
    refresh = "true", 
    body = sprintf('{
        "query": {
          "ids": {
            "values": [%s]
          }
        },
        "script": {
          "params": {
            "fieldToChange": "tag",
            "newValue": "%s"
          },
          "source": "
            if (ctx._source[params.fieldToChange] == null) {
              ctx._source[params.fieldToChange] = [params.newValue];
            } else if (!(ctx._source[params.fieldToChange] instanceof List)) {
              ctx._source[params.fieldToChange] = [ctx._source[params.fieldToChange]];
              ctx._source[params.fieldToChange].add(params.newValue);
            } else {
              ctx._source[params.fieldToChange].add(params.newValue)
            }
            "
        }
      } ', 
        id_string,
        tagToAdd
    )
  )
  logger::log_info("Completed update on {res_update$updated} docs")
  invisible(res_update$updated)
}

# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.
