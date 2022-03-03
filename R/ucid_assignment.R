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
#' @param ufidLevel numeric length 1, 1 or 2
#'
#' @return TRUE if ran to completion
#' @export
es_assign_ucids <- function(escon, index = "g2_nts*", rtlim = 0.2, ufidLevel = 1) {
  if (index != "g2_nts*")
    warning("If not all indices at once, ucids will not match between indicies")
  
  # ufidLevel <- 2
  # rtlim = 0.2
  stopifnot(is.numeric(ufidLevel), length(ufidLevel) == 1, is.element(ufidLevel, c(1,2)))
  ufidType <- ifelse(ufidLevel == 1, "ufid", "ufid2")
  ucidType <- ifelse(ufidLevel == 1, "ucid", "ucid2")

  # delete all previous ucids
  response <- elastic::docs_update_by_query(
    escon, index,
    body =
    sprintf('
    {
      "query": {
        "exists": {
          "field": "%s"
        }
      },
      "script": {
        "source": "ctx._source.remove(\'%s\')",
        "lang": "painless"
      }
    }
    ', ucidType, ucidType)
  )

  if (response$timed_out)
    stop("error while clearing previous ucids (timed out)")

  message(response$total, " docs were cleared of ", ucidType)

  # first gather rt information since you can skip most comparisions just by
  # looking at rt difference

  rtres <- elastic::Search(
    escon, index, body =
      sprintf('
    {
      "query": {
        "match_all": {}
      },
      "size": 0,
      "aggs": {
        "ufids": {
          "terms": {
            "field": "%s",
            "size": 100000,
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
    ', ufidType))
  stopifnot(rtres$aggregations$ufids$sum_other_doc_count == 0)
  rtc <- rtres$aggregations$ufids$buckets
  allUfids <- vapply(rtc, "[[", numeric(1), i = "key")
  ufidRts <- vapply(rtc, function(x) x$rt$value, numeric(1))
  
  # allUfids <- allUfids[500:1000]
  # ufidRts <- ufidRts[500:1000]

  # make rts contiguous so that you can search rtm by index rather than name (much faster)
  df <- data.frame(ufid = allUfids, rt = ufidRts)
  nums <- data.frame(ufid = seq_len(max(allUfids)))
  df2 <- merge(nums, df, all.x = T, by = "ufid")
  df2[is.na(df2$rt), "rt"] <- 0

  rtd <- parallelDist::parDist(as.matrix(df2$rt), method = "euclidean", threads = 6)
  message("size of rt dist object: ", format(object.size(rtd), units = "MB", standard = "SI"))
  nn <- length(df2$rt)
  
  rowcol <- function(ix, n) { #given index, return row and column
    nr <- ceiling(n-(1+sqrt(1+4*(n^2-n-2*ix)))/2)
    nc <- n-(2*n-nr+1)*nr/2+ix+nr
    c(nr,nc)
  }
  
  pairs <- lapply(which(rtd <= rtlim), rowcol, n = nn)
  rm(rtd)
  
  pairs <- split(pairs, ceiling(seq_along(pairs)/1000))
  process_chunk <- function(chunk) {  # chunk <- pairs[[1]]
    pairsMat <- do.call("rbind", chunk)
    pairsFilt <- if (ufidType == "ufid") {
      lapply(chunk, function(x) list(terms = list(ufid = list(x[1], x[2]))))
    } else if (ufidType == "ufid2") {
      lapply(chunk, function(x) list(terms = list(ufid2 = list(x[1], x[2]))))
    } else {
      stop("ufidType not found")
    }
    chunkRes <- elastic::Search(
      escon, index, size = 0, 
      body = list(
        query = list(
          exists = list(
            field = "ufid2"
          )
        ),
        aggs = list(
          pairs = list(
            filters = list(
              filters = pairsFilt
            ),
            aggs = list(
              batches = list(
                terms = list(
                  field = "date_import",
                  size = 1000
                ),
                aggs = list(
                  components = list(
                    terms = list(
                      field = "tag",
                      size = 1000
                    ),
                    aggs = list(
                      ufids = list(
                        cardinality = list(
                          field = "ufid2"
                        )
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
    cr <- chunkRes$aggregations$pairs$buckets
    
    compare_pair <- function(pairsbucket) {
      totalDocs <- pairsbucket$doc_count
      buckets <- pairsbucket$batches$buckets
      totalFoundTogether <- sum(unlist(lapply(buckets, function(b) {
        vapply(b$components$buckets, function(c) {
          if (grepl("^component_\\d+$", c$key) && c$ufids$value == 2)
            c$doc_count else 0
        }, numeric(1))
      })))
      round(totalFoundTogether / totalDocs, 2)
    }
    pairsMat <- cbind(pairsMat, vapply(cr, compare_pair, numeric(1)))
    pairsMat
  } 
  # takes a long time
  message("Starting ucid distance matrix computation on ", date())
  pairs <- lapply(pairs, process_chunk)
  pairs <- do.call("rbind", pairs)
  pairs <- pairs[pairs[, 3] != 0, ]
  pairs <- pairs[!is.na(pairs[, 3]), ]
  # save in case of crash
  saveRDS(pairs, "~/temp/ucid_obj_pairs.RDS")
  # pairs <- readRDS("~/temp/ucid_obj_pairs.RDS")
  pairsVec <- unique(c(pairs[,1], pairs[,2]))
  get_ufid_comparison <- function(u1, u2, pairs, pairsVec) {
    # u1 <- 127
    # u2 <- 456
    if (is.element(u1, pairsVec) && is.element(u2, pairsVec)) {
      num <- pairs[pairs[,1] == u1 & pairs[,2] == u2, 3]
      if (length(num) != 0) {
        num
      } else {
        num2 <- pairs[pairs[,1] == u2 & pairs[,2] == u1, 3]
        if (length(num2) != 0) {
          num2
        } else {
          0
        }
      }
    } else {
      0
    }
  }
  message("Starting dist_make on ", date())
  #m <- dist_make_parallel(as.matrix(allUfids), get_ufid_comparison, 20, pairs = pairs, pairsVec = pairsVec)
  m <- usedist::dist_make(as.matrix(allUfids), get_ufid_comparison)
  saveRDS(m, "~/temp/ucid_obj_m.RDS")
  message("Completed on ", date())
  message("size of dist object: ", format(object.size(m), units = "MB", standard = "SI"))
  m2 <- 1-m
  rm(m)
  #dbscan::kNNdistplot(m2, 3)
  clus <- dbscan::dbscan(m2, 0.2, 2)  # more than 80% of features must be matching
  rm(m2)
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
            "%s": {
              "value": %i
            }
          }
        },
        "script": {
          "source": "ctx._source.%s = params.newUcid",
          "lang": "painless",
          "params": {
            "newUcid": %i
          }
        }
      }
      ', ufidType, uf, ucidType, uc)
    )

    message("completed update of ", ufidType, " ", uf)
  }
  message("All updates complete.")
  TRUE
}
