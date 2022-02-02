# Function to analyze ES DB for testing purposes

#' Compute false positives and false negatives in ufid and ucid assignment
#'
#' Used for testing purposes
#'
#' @param escon
#' @param udb
#' @param index
#'
#' @return list of metrics used to assess quality of ufid and ucid assignments
#' @export
es_test_fpfn <- function(escon, udb, index) {
  #index <- "g2_nts*"
  # get total number of features

  totFeat <- elastic::count(escon, index = index)
  totFeatMs2 <- elastic::Search(escon, index = index,size = 0, body =
  '
    {
      "query": {
        "nested": {
          "path": "ms2",
          "query": {
            "exists": {
              "field": "ms2.mz"
            }
          }
        }
      }
    }
  ')
  totFeatMs2 <- totFeatMs2$hits$total$value

  ms2percent <- 100 * totFeatMs2 / totFeat

  # number of features with ufid

  totWithUfid <- elastic::Search(
    escon, index, size = 0, body =
    '
      {
        "query": {
          "exists": {
            "field": "ufid"
          }
        }
      }
    '
  )
  totWithUfid <- totWithUfid$hits$total$value
  percentWithUfid <- 100 * totWithUfid / totFeat

  totMs2withUfid <- elastic::Search(
    escon, index, size = 0, body =
      '
      {
        "query": {
          "bool": {
            "filter": [
              {
                "exists": {
                  "field": "ufid"
                }
              },
              {
                "nested": {
                  "path": "ms2",
                  "query": {
                    "exists": {
                      "field": "ms2.mz"
                    }
                  }
                }
              }
            ]
          }
        }
      }
    '
  )
  totMs2withUfid <- totMs2withUfid$hits$total$value
  percentMs2withUfid <- 100 * totMs2withUfid / totFeatMs2

  # Calculation of fP ####
  # FP Flavor 1 ####
  # can only be done on annotated features with ufid
  totUfidAnnotated <- elastic::Search(
    escon, index, size = 0, body =
      '
      {
        "query": {
          "bool": {
            "filter": [
              {
                "exists": {
                  "field": "name"
                }
              },
              {
                "exists": {
                  "field": "ufid"
                }
              }
            ]
          }
        }
      }
    '
  )$hits$total$value


  # which of these ufids have more than one name, assume name with highest frequency is
  # correct and all other features are fp
  nameCardinality <- elastic::Search(escon, index, body = '
                                     {
  "query": {
    "bool": {
      "filter": [
        {
          "exists": {
            "field": "name"
          }
        },
        {
          "exists": {
            "field": "ufid"
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "ufids": {
      "terms": {
        "field": "ufid",
        "size": 10000
      },
      "aggs": {
        "names": {
          "cardinality": {
            "field": "name"
          }
        },
        "select_buckets": {
          "bucket_selector": {
            "buckets_path": {
              "nameCardinality": "names"
            },
            "script": "params.nameCardinality > 1"
          }
        }
      }
    }
  }
}
                                     ')$aggregations$ufids$buckets

  # which ufids?
  problemUfids <- sapply(nameCardinality, function(x) x$key)

  # for each of these ufids, determine the number of features not belonging to
  # the name with the highest frequency,  everything else is fp
  totFPFeatures1 <- sum(vapply(problemUfids, function(uf) {
    featuresPerName <- elastic::Search(escon, index, body = sprintf('
                    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "size": 0,
      "aggs": {
        "namesCount": {
          "terms": {
            "field": "name",
            "size": 10
          }
        }
      }
    }
  ', uf))$aggregations$namesCount$buckets

    featuresPerName <- sapply(featuresPerName, function(x) x$doc_count)
    # results are already sorted by size (first group is the largest)
    sum(featuresPerName[-1])
  }, numeric(1)))

  problemUfidsNames <- lapply(problemUfids, function(uf) {
    featuresPerName <- elastic::Search(escon, index, body = sprintf('
                    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "size": 0,
      "aggs": {
        "namesCount": {
          "terms": {
            "field": "name",
            "size": 10
          }
        }
      }
    }
  ', uf))$aggregations$namesCount$buckets

    sapply(featuresPerName, function(x) x$key)
  })
  names(problemUfidsNames) <- paste("ufid", problemUfids, sep = "_")

  # FP flavor 2 ####
  # second type of FPs are features belonging to one name but with multiple ufids
  # must be done for each polarity separately, since each polarity has a different ufid
  get_ufid_cardinality <- function(polarity) {
    ufidCardinality <- elastic::Search(escon, index, body = sprintf('
                                     {
  "query": {
    "bool": {
      "filter": [
        {
          "exists": {
            "field": "name"
          }
        },
        {
          "exists": {
            "field": "ufid"
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
  "size": 0,
  "aggs": {
    "names": {
      "terms": {
        "field": "name",
        "size": 10000
      },
      "aggs": {
        "ufids": {
          "cardinality": {
            "field": "ufid"
          }
        },
        "select_buckets": {
          "bucket_selector": {
            "buckets_path": {
              "ufidCardinality": "ufids"
            },
            "script": "params.ufidCardinality > 1"
          }
        }
      }
    }
  }
}
                                     ', polarity))$aggregations$names$buckets
    # which names?
    problemNames <- sapply(ufidCardinality, function(x) x$key)
    # for each of these names, determine the number of features not belonging to
    # the ufid with the highest frequency, everything else is fp
    totFPFeatures2 <- sum(vapply(problemNames, function(nm) {
      featuresPerUfid <- elastic::Search(escon, index, body = sprintf('
      {
      "query": {
        "bool":{
          "filter":[
            {
              "term": {
                "name": {
                  "value": "%s"
                }
              }
            },
            {
              "term": {
                "pol":{"value": "%s"}
              }
            }
          ]
        }
      },
      "size": 0,
      "aggs": {
        "ufidsCount": {
          "terms": {
            "field": "ufid",
            "size": 100
          }
        }
      }
    }
  ', nm, polarity))$aggregations$ufidsCount$buckets

      featuresPerUfid <- sapply(featuresPerUfid, function(x) x$doc_count)
      # results are already sorted by size (first group is the largest)
      sum(featuresPerUfid[-1])
    }, numeric(1)))

    list(problemNames = problemNames, totFPFeatures2 = totFPFeatures2)
  }
  type2FpPos <- get_ufid_cardinality("pos")
  type2FpNeg <- get_ufid_cardinality("neg")

  problemNames <- c(type2FpPos$problemNames, type2FpNeg$problemNames)
  totFPFeatures2 <- sum(type2FpPos$totFPFeatures2, type2FpNeg$totFPFeatures2)

  allUfids <- tbl(udb, "feature") %>% select(ufid) %>% collect() %>% .$ufid

  # for each ufid, get number of docs which are result of multiple assignment in one sample
  get_docs_multi_assignment <- function(ufid) {
    res <- elastic::Search(
    escon, index, body = sprintf(
    '
    {
      "query": {
        "term": {
          "ufid": {
            "value": %i
          }
        }
      },
      "size": 0,
      "aggs": {
        "files": {
          "terms": {
            "field": "filename",
            "size": 10000
          }
        }
      }
    }
    ', ufid))
    stopifnot(res$aggregations$files$sum_other_doc_count == 0)

    b <- res$aggregations$files$buckets
    # there should be one feature for each file, all others are fp
    sum(vapply(b, function(x) x$doc_count, numeric(1)) - 1)
  }
  numberMultiAssign <- sum(vapply(allUfids, get_docs_multi_assignment, numeric(1)))


  # ucid accuracy ####

  # compute max sd in rt across all ucids

  ucid_rts <- elastic::Search(
    escon, index, body =
    '
    {
      "query": {
        "exists": {
          "field": "ucid"
        }
      },
      "size": 0,
      "aggs": {
        "ucids": {
          "terms": {
            "field": "ucid",
            "size": 10000
          },
          "aggs": {
            "rt": {
              "extended_stats": {
                "field": "rt"
              }
            }
          }
        }
      }
    }
    '
  )$aggregations$ucids$buckets
  numUcids <- length(ucid_rts)
  mxRtDevUcid <- max(vapply(ucid_rts, function(x) x$rt$std_deviation, numeric(1)))


  list(
    total_features = totFeat,
    total_features_with_ufid = totWithUfid,
    percent_features_with_ufid = round(percentWithUfid),
    percent_fn = 100 - round(percentWithUfid),
    total_features_with_ms2 = totFeatMs2,
    percent_with_ms2 = round(ms2percent),
    percent_ms2_features_with_ufid = round(percentMs2withUfid),
    percent_ms2_fn = 100 - round(percentMs2withUfid),
    total_features_multiple_assignment = numberMultiAssign,
    percent_ufids_multiple_assignment = round(100 * numberMultiAssign / totWithUfid),
    total_features_ufid_annotated = totUfidAnnotated,
    percent_fp_multiple_names_by_ufid = round(100 * totFPFeatures1 / totUfidAnnotated),
    percent_fp_multiple_ufids_by_name = round(100 * totFPFeatures2 / totUfidAnnotated),
    percent_fp_sum = round(100 * (totFPFeatures1 + totFPFeatures2) / totUfidAnnotated),
    percent_fp_avg = round(100 * mean(c(totFPFeatures1 / totUfidAnnotated,  totFPFeatures2 / totUfidAnnotated))),
    num_ucids = numUcids,
    max_rt_stddev_ucid = round(mxRtDevUcid, 2),
    names_multiple_ufids = problemNames,
    ufids_multiple_names = problemUfidsNames
    )
}



