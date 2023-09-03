# Functions to add data to documents in NTSPortal

#' Add rtt field to all documents where this does not exist
#' 
#' 
#'
#' @param escon connection to ElasticSearch
#' @param esindex Index or index-pattern to update
#'
#' @return Number of updated docs (invisibly)
#' @export
#'
es_add_rtt <- function(escon, esindex) {
  # Run script to add rtt to any docs where this does not exist.
  # check that it does not exist
  
  checkRes <- elastic::Search(escon, index = esindex, body = '
  {
    "query": {
      "nested": {
        "path": "rtt",
        "query": {
          "bool": {
            "must_not": [
              {
                "exists": {
                  "field": "rtt.rt"
                }
              }
            ]
          }
        }
      }
    },
    "size": 0
  }
  ')
  
  totalNoRtt <- checkRes$hits$total$value
  
  if (totalNoRtt > 0) {
    res <- elastic::docs_update_by_query(escon, index = esindex, body = '
  {
    "query": {
      "bool": {
        "filter": [
          {
            "exists": {
              "field": "rt"
            }
          },
          {
            "exists": {
              "field": "chrom_method"
            }
          }
        ],
        "must_not": [
          {
            "nested": {
              "path": "rtt",
              "query": {
                "term": {
                  "rtt.predicted": {
                    "value": false
                  }
                }
              }
            }
          }
        ]
      }
    },
    "script": {
      "source" : "
        params.entry[0].put(\'rt\', ctx._source[\'rt\']);
        params.entry[0].put(\'method\', ctx._source[\'chrom_method\']);
        if (ctx._source.rtt != null) {
          ctx._source.rtt.add(params.entry);
        } else {
          ctx._source.rtt = params.entry;
        }
      ",
      "params" : {
        "entry" : [
          {
            "predicted" : false
          }
        ]
      }
    } 
  }')
    
    log_info("Completed update on {res$updated} docs")
    invisible(res$updated)
  } else {
    log_info("No docs to update.")
    invisible(0L)
  }
}


#' Add rt_clustering field to es index pattern
#'
#' @param escon connection to ElasticSearch
#' @param esindex Index or index-pattern to update
#'
#' @return NULL
#' @export
#'
#' @import logger
es_add_rt_cluster <- function(escon, esindex) {
  # get all features without rt_clustering
  totUpdated <- 0
  repeat {
    res <- elastic::Search(escon, index, body = '
     {
      "query": {
        "bool": {
          "must_not": [
            {
              "exists": {
                "field": "rt_clustering"
              }
            }
          ]
        }
      },
      "_source": ["rtt"],
      "size": 10000
    }
                       
                       ')
    if (res$hits$total$value == 0)
      break
    
    log_info("There are {res$hits$total$value} left to process")  
    
    fts <- res$hits$hits
    for (ft in fts) { #ft <- fts[[1]]
      # get rt
      doc <- ft[["_source"]]
      esid <- ft[["_id"]]
      esind <- ft[["_index"]]
      rtToCopy <- doc$rtt[[which(sapply(doc$rtt, function(x) x$method == "bfg_nts_rp1"))]]$rt
      elastic::docs_update(escon, esind, esid, body = sprintf('
    {
      "script" : {
        "source": "ctx._source.rt_clustering = params.rtToCopy;",
        "params": {
          "rtToCopy": %.2f
        }
      }
    }
    ', rtToCopy))
    }
    totUpdated <- sum(totUpdated, res$hits$total$value)
  }
  
  log_info("Completed all features")
  invisible(totUpdated)
}
