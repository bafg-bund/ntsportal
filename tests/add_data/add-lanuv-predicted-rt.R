

# Add predicted lanuv retention times using GAM model

# will take a long time so run it using
# nohup Rscript add-lanuv-predicted-rt.R &> add-lanuv-predicted-rt-221223.log &

# TODO add rt_clustering field while you are at it. The rt field is needed at the top level for sorting
# during clustering

library(mgcv)
library(logger)
source("~/connect-ntsp.R")

VERSION <- "2022-12-23"

log_info("-------- add-lanuv-predicted-rt.R v{VERSION} -------")

gmod <- readRDS("model-lanuv-gam-v3.RDS")

add_rt <- function(docId, docInd) { # docId <- ids[1]
  #docInd <- indices[1]
  # retrieve experimental rt
  rttl <- elastic::docs_get(escon, index = docInd, id = docId, source = "rtt", 
                            verbose = F)[["_source"]][["rtt"]]
  rtt <- do.call("rbind", lapply(rttl, as.data.frame))
  thisRt <- rtt[rtt$method == "bfg_nts_rp1", "rt"]
  # calculate new RT using GAM
  nd <- data.frame(bfgrt = thisRt)
  nd$rt <- predict(gmod, newdata = nd)
  newRt <- nd[1, "rt"]
  stopifnot(is.numeric(newRt), !is.na(newRt), newRt > 0)
  # add new RT to doc
  retval <- elastic::docs_update(
    escon, docInd, docId,
    body = sprintf('
    {
      "script": {
        "source": "ctx._source.rtt.add(params.entry)",
        "params" : {
          "entry": {
            "predicted": true,
            "rt": %.2f,
            "method": "lanuv_nts"
          }
        }
      }
    }
    ', newRt))
  if (retval$result != "updated")
    stop("Error updating ", docId, " in ", docInd)
  TRUE
}

# Retrieve ids of documents which have experimental retention time bfg_nts_rp1 in the rtt field
# and do not yet have a predicted retention time for lanuv_nts
log_info("Starting processing")
repeat {
  res <- elastic::Search(
    escon, "g2_nts*",
    body = '
    {
      "query": {
        "bool": {
          "filter": [
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
            },
            {
              "nested": {
                "path": "rtt",
                "query": {
                  "term": {
                    "rtt.method": {
                      "value": "bfg_nts_rp1"
                    }
                  }
                }
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
                      "value": true
                    }
                  }
                }
              }
            },
            {
              "nested": {
                "path": "rtt",
                "query": {
                  "term": {
                    "rtt.method": {
                      "value": "lanuv_nts"
                    }
                  }
                }
              }
            }
          ]
        }
      },
      "fields": [],
      "_source": false, 
      "size": 10000
    }
    '
    )
  # will keep repeating until there are no docs left
  if (res$hits$total$value == 0)
    break
  
  log_info("There are {res$hits$total$value} docs left to process in total")
  log_info("Processing the next {length(res$hits$hits)} docs")
  ids <- vapply(res$hits$hits, "[[", i = "_id", character(1))
  indices <- vapply(res$hits$hits, "[[", i = "_index", character(1))
  ok <- mapply(add_rt, ids, indices)
  if (all(ok)) {
    log_info("Completed {length(res$hits$hits)} docs, repeating loop")
  }
}

log_info("All docs completed, end of script")
