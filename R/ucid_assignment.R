# function for ucid assignment


#' Assign ucids to all documents
#'
#' Totally new assignment, will delete previous ucids
#'
#' build distance matrix of all ufids against eachother
#' each element of the matrix says how often the two ufids
#' are in the same component per batch.
#'
#' @param escon
#' @param index should be all indices together otherwise ucids won't match across indices
#' @param rtlim pre-eliminate ufids from consideration which have rt difference higher than this
#'
#' @return TRUE if ran to completion
#' @export
es_assign_ucids <- function(escon, index = "g2_nts*", rtlim = 0.2) {
  if (index != "g2_nts*")
    warning("If not all indices at once, ucids will not match between indicies")

  # delete all previous ucids
  response <- elastic::docs_update_by_query(
    escon, index,
    body =
    '
    {
      "query": {
        "exists": {
          "field": "ucid"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'ucid\')",
        "lang": "painless"
      }
    }
    '
  )

  if (response$timed_out)
    stop("error while clearing previous ucids (timed out)")

  message(response$total, " docs were cleared of ucid")


  # first gather rt information since you can skip most comparisions just by
  # looking at rt difference

  rtres <- elastic::Search(
    escon, index, body =
      '
    {
  "query": {
    "match_all": {}
  },
  "size": 0,
  "aggs": {
    "ufids": {
      "terms": {
        "field": "ufid",
        "size": 10000,
        "order": {
          "_key": "asc"
        }
      },
      "aggs": {
        "rt": {
          "avg": {
            "field": "rt"
          }
        }
      }
    }
  }
}
    ')
  stopifnot(rtres$aggregations$ufids$sum_other_doc_count == 0)
  rtc <- rtres$aggregations$ufids$buckets
  allUfids <- vapply(rtc, "[[", numeric(1), i = "key")
  ufidRts <- vapply(rtc, function(x) x$rt$value, numeric(1))

  # make rts contiguous so that you can search rtm by index rather than name (much faster)
  df <- data.frame(ufid = allUfids, rt = ufidRts)
  nums <- data.frame(ufid = seq_len(max(allUfids)))
  df2 <- merge(nums, df, all.x = T, by = "ufid")
  df2[is.na(df2$rt), "rt"] <- 0

  rtd <- parallelDist::parDist(as.matrix(df2$rt), method = "euclidean", threads = 6)
  rtm <- as.matrix(rtd)

  compare_ufids <- function(u1, u2) {
    # if rtdiff large then you can return 0 without calling elasticsearch
    if (rtm[u1,u2] > rtlim)
      return(0)
    # aggregate by batches, in each batch count the number of components that
    # contain both ufids. The number of features in these components is
    # summed and given as a fraction of the total number of features.
    r <- elastic::Search(escon, index, body = sprintf(
    '
    {
      "query": {
        "terms": {
          "ufid": [
            %i,
            %i
          ]
        }
      },
      "size": 0,
      "aggs": {
        "batches": {
          "terms": {
            "field": "date_import",
            "size": 200
          },
          "aggs": {
            "components": {
              "terms": {
                "field": "tag",
                "size": 100
              },
              "aggs": {
                "ufids": {
                  "cardinality": {
                    "field": "ufid"
                  }
                }
              }
            }
          }
        }
      }
    }
    ', u1, u2))
    totalDocs <- r$hits$total$value
    if (r$aggregations$batches$sum_other_doc_count != 0)
      warning("docs left out of ucid determination")
    buckets <- r$aggregations$batches$buckets
    totalFoundTogether <- sum(unlist(lapply(buckets, function(b) {
      vapply(b$components$buckets, function(c) {
        if (c$ufids$value == 2)
          c$doc_count else 0
      }, numeric(1))
    })))
    round(totalFoundTogether / totalDocs, 2)
  }

  # takes a long time
  message("Starting ucid distance matrix computation on ", date())
  m <- usedist::dist_make(as.matrix(allUfids), compare_ufids)
  message("Completed on ", date())
  message("size of dist object: ", format(object.size(m), units = "MB", standard = "SI"))
  m2 <- 1-m
  #dbscan::kNNdistplot(m2, 3)
  clus <- dbscan::dbscan(m2, 0.2, 2)  # more than 80% of features must be matching
  assignments <- data.frame(ufid = allUfids, ucid = clus$cluster)

  # assign the ucids to documents
  # later, this needs to be done by recording the ucids in the ufid database
  # but for now not enough experience for this.
  # just recompute the ucids every time. And do a complete new assignment
  # need to do this for all indices individually
  for (i in seq_len(nrow(assignments))) {
    uc <- assignments[i, "ucid"]
    if (uc == 0)
      next
    uf <- assignments[i, "ufid"]

    elastic::docs_update_by_query(escon, index, body = sprintf(
      '
      {
        "query": {
          "term": {
            "ufid": {
              "value": %i
            }
          }
        },
        "script": {
          "source": "ctx._source.ucid = params.newUcid",
          "lang": "painless",
          "params": {
            "newUcid": %i
          }
        }
      }
      ', uf, uc)
    )

    message("completed update of ufid ", uf)
  }
  message("All updates complete.")
  TRUE
}
