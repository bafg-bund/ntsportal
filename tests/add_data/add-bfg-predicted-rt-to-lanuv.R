

# Add predicted bfg retention times using inverted GAM model

# will take a long time so run it using
# nohup Rscript add-bfg-predicted-rt-to-lanuv.R &> logs/add-bfg-predicted-rt-to-lanuv-230104.log &


# This is the inverse of add-lanuv-predicted-rt.R
# in the future the script should be able to predict rts based on any experimental data.
# could also be made dynamic, recomputing the GAM based on current data


library(mgcv)
library(logger)
source("~/connect-ntsp.R")

VERSION <- "2023-01-04"

log_info("-------- add-bfg-predicted-rt-to-lanuv.R v{VERSION} -------")

gmod <- readRDS("model-lanuv-gam-v3-inverse.RDS")

add_rt <- function(docId, docInd) { # docId <- ids[1]
  #docInd <- indices[1]
  # retrieve experimental rt
  rttl <- elastic::docs_get(escon, index = docInd, id = docId, source = "rtt", 
                            verbose = F)[["_source"]][["rtt"]]
  rtt <- do.call("rbind", lapply(rttl, as.data.frame))
  thisRt <- rtt[rtt$method == "lanuv_nts", "rt"]
  # calculate new RT using GAM
  nd <- data.frame(rt.lanuv = thisRt)
  nd$rt <- predict(gmod, newdata = nd)
  newRt <- nd[1, "rt"]
  stopifnot(is.numeric(newRt), !is.na(newRt), newRt > 0)
  # add new RT to doc
  # script must be on one line
  retval <- elastic::docs_update(
    escon, docInd, docId,
    body = sprintf('
    {
      "script": {
        "source": "ctx._source.rtt.add(params.entry); ctx._source.rt_clustering = params.rtNew;",
        "params" : {
          "entry": {
            "predicted": true,
            "rt": %.2f,
            "method": "bfg_nts_rp1"
          },
          "rtNew": %.2f
        }
      }
    }
    ', newRt, newRt))
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
                      "value": "lanuv_nts"
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
                      "value": "bfg_nts_rp1"
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