# Function to analyze ES DB for testing purposes

#' Compute false positives and false negatives in ufid and ucid assignment
#'
#' Used for testing purposes
#'
#' @param escon
#' @param index
#'
#' @return list of metrics used to assess quality of ufid and ucid assignments
#' @export
es_test_fpfn <- function(escon, index) {
  
  # index <- "g2_nts*"
  # config_path <- "~/config.yml"
  # ec <- config::get("elastic_connect", file = config_path)
  # escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)
   
  # get total number of features
  message("Starting tests at ", date())
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
  
  tot_with_ufid <- function(ufidType, perc) {
    totWithUfid <- elastic::Search(
      escon, index, size = 0, body = sprintf(
        '
      {
        "query": {
          "exists": {
            "field": "%s"
          }
        }
      }
    ', ufidType)
    )
    totl <- totWithUfid$hits$total$value
    if (perc)
      100 * totl / totFeat else totl
  }

  percentWithUfid <- tot_with_ufid("ufid", TRUE)
  percentWithUfid2 <- tot_with_ufid("ufid2", TRUE)
  totWithUfid <- tot_with_ufid("ufid", FALSE)
  totWithUfid2 <- tot_with_ufid("ufid2", FALSE)
  
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
  get_tot_ufid_annotated <- function(ufidType) {
    elastic::Search(
      escon, index, size = 0, body = sprintf(
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
                  "field": "%s"
                }
              }
            ]
          }
        }
      }
    ', ufidType)
    )$hits$total$value
  }
  totUfidAnnotated <- get_tot_ufid_annotated("ufid")
  totUfid2Annotated <- get_tot_ufid_annotated("ufid2")

  # which of these ufids have more than one name, assume name with highest frequency is
  # correct and all other features are fp
  problem_ufids <- function(ufidType) {
    nameCardinality <- elastic::Search(escon, index, body = sprintf('
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
              "field": "%s"
            }
          }
        ]
      }
    },
    "size": 0,
    "aggs": {
      "ufids": {
        "terms": {
          "field": "%s",
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
  ', ufidType, ufidType))$aggregations$ufids$buckets
    
    # which ufids?
    sapply(nameCardinality, function(x) x$key)
  }
  
  problemUfids <- problem_ufids("ufid")
  problemUfid2s <- problem_ufids("ufid2")

  # for each of these ufids, determine the number of features not belonging to
  # the name with the highest frequency,  everything else is fp
  tot_fp_features_1 <- function(probUfids, ufidType) {
    sum(vapply(probUfids, function(uf) {
      featuresPerName <- elastic::Search(escon, index, body = sprintf('
                    {
      "query": {
        "term": {
          "%s": {
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
  ', ufidType, uf))$aggregations$namesCount$buckets
      
      featuresPerName <- sapply(featuresPerName, function(x) x$doc_count)
      # results are already sorted by size (first group is the largest)
      sum(featuresPerName[-1])
    }, numeric(1)))
  }
  totFPFeatures1ufid <- tot_fp_features_1(problemUfids, "ufid")
  totFPFeatures1ufid2 <- tot_fp_features_1(problemUfid2s, "ufid2")
  
  problem_ufids_names <- function(probUfids, ufidType) {
    probUfidsNames <- lapply(probUfids, function(uf) {
      featuresPerName <- elastic::Search(escon, index, body = sprintf('
                    {
      "query": {
        "term": {
          "%s": {
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
    ', ufidType, uf))$aggregations$namesCount$buckets
      
      sapply(featuresPerName, function(x) x$key)
    })
    names(probUfidsNames) <- paste(ufidType, probUfids, sep = "_")
    probUfidsNames
  }
  problemUfidsNames <- problem_ufids_names(problemUfids, "ufid")
  problemUfid2sNames <- problem_ufids_names(problemUfid2s, "ufid2")

  # FP flavor 2 ####
  # second type of FPs are features belonging to one name but with multiple ufids
  # must be done for each polarity separately, since each polarity has a different ufid
  get_ufid_cardinality <- function(polarity, ufidType) {
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
                "field": "%s"
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
                "field": "%s"
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
    ', ufidType, polarity, ufidType))$aggregations$names$buckets
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
            "field": "%s",
            "size": 100
          }
        }
      }
    }
  ', nm, polarity, ufidType))$aggregations$ufidsCount$buckets

      featuresPerUfid <- sapply(featuresPerUfid, function(x) x$doc_count)
      # results are already sorted by size (first group is the largest)
      sum(featuresPerUfid[-1])
    }, numeric(1)))

    list(problemNames = problemNames, totFPFeatures2 = totFPFeatures2)
  }
  type2FpPos <- get_ufid_cardinality("pos", "ufid")
  type2FpNeg <- get_ufid_cardinality("neg", "ufid")
  type2FpPosUfid2 <- get_ufid_cardinality("pos", "ufid2")
  type2FpNegUfid2 <- get_ufid_cardinality("neg", "ufid2")

  problemNames <- c(type2FpPos$problemNames, type2FpNeg$problemNames)
  totFPFeatures2 <- sum(type2FpPos$totFPFeatures2, type2FpNeg$totFPFeatures2)
  problemNamesUfid2 <- c(type2FpPosUfid2$problemNames, type2FpNegUfid2$problemNames)
  totFPFeatures2Ufid2 <- sum(type2FpPosUfid2$totFPFeatures2, type2FpNegUfid2$totFPFeatures2)

  # multiple ufids in one sample ####
  
  # get all ufids
  get_all_ufids <- function(ufidType) {
    ufidres <- elastic::Search(
    escon, index, body = sprintf(
      '
    {
      "query": {
        "exists": {
          "field": "%s"
        }
      },
      "size": 0,
      "aggs": {
        "ufids": {
          "terms": {
            "field": "%s",
            "size": 1000000
          }
        }
      }
    }
      ', ufidType, ufidType)
    )
    if (ufidres$aggregations$ufids$sum_other_doc_count != 0)
      stop("not all ", ufidType, "s were counted when getting multiple assignments")
    vapply(ufidres$aggregations$ufids$buckets, "[[", numeric(1), i = "key")
  }
  allUfids <- get_all_ufids("ufid")
  allUfid2s <- get_all_ufids("ufid2") 
  
  # for each ufid, get number of docs which are result of multiple assignment in one sample
  get_docs_multi_assignment <- function(ufid, ufidType) {
    res <- elastic::Search(
    escon, index, body = sprintf(
    '
    {
      "query": {
        "term": {
          "%s": {
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
    ', ufidType, ufid))
    stopifnot(res$aggregations$files$sum_other_doc_count == 0)

    b <- res$aggregations$files$buckets
    # there should be one feature for each file, all others are fp
    sum(vapply(b, function(x) x$doc_count, numeric(1)) - 1)
  }
  message("Computing duplicated ufid assignments to one sample on ", date())
  numberMultiAssignUfid <- sum(
    vapply(
      allUfids, 
      get_docs_multi_assignment, 
      numeric(1), 
      ufidType = "ufid"
    )
  )
  message("Computing duplicated ufid2 assignments to one sample on ", date())
  numberMultiAssignUfid2 <- sum(
    vapply(
      allUfid2s, 
      get_docs_multi_assignment, 
      numeric(1), 
      ufidType = "ufid2"
    )
  )
  
  
  # ucid accuracy ####

  # compute max sd in rt across all ucids
  message("Computing ucid accuracy on ", date())
  
  test_ucid <- function(ucidType) {
   ucid_rts <- elastic::Search(
    escon, index, body = sprintf(
    '
    {
      "query": {
        "exists": {
          "field": "%s"
        }
      },
      "size": 0,
      "aggs": {
        "ucids": {
          "terms": {
            "field": "%s",
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
    ', ucidType, ucidType)
  )$aggregations$ucids$buckets
  numUcids <- length(ucid_rts)
  mxRtDevUcid <- max(vapply(ucid_rts, function(x) x$rt$std_deviation, numeric(1))) 
  list(numUcids = numUcids, mxRtDevUcid = mxRtDevUcid)
  }
  ucidList <- test_ucid("ucid")
  mxRtDevUcid <- ucidList$mxRtDevUcid
  numUcids <- ucidList$numUcids
  ucid2List <- test_ucid("ucid2")
  mxRtDevUcid2 <- ucid2List$mxRtDevUcid
  numUcid2s <- ucid2List$numUcids

  message("Completed tests at ", date())
  # Output List ####
  list(
    total_features = totFeat,
    percent_fn_ufid = 100 - round(percentWithUfid),
    percent_fn_ufid2 = 100 - round(percentWithUfid2),
    total_features_with_ms2 = totFeatMs2,
    percent_with_ms2 = round(ms2percent),
    percent_ms2_fn_ufid = 100 - round(percentMs2withUfid),
    percent_ufids_multiple_assignment = round(100 * numberMultiAssignUfid / totWithUfid),
    percent_ufid2s_multiple_assignment = round(100 * numberMultiAssignUfid2 / totWithUfid2),
    total_features_ufid_annotated = totUfidAnnotated,
    total_features_ufid2_annotated = totUfid2Annotated,
    percent_fp_multiple_names_by_ufid = round(100 * totFPFeatures1ufid / totUfidAnnotated),
    percent_fp_multiple_names_by_ufid2 = round(100 * totFPFeatures1ufid2 / totUfid2Annotated),
    percent_fp_multiple_ufids_by_name = round(100 * totFPFeatures2 / totUfidAnnotated),
    percent_fp_multiple_ufid2s_by_name = round(100 * totFPFeatures2Ufid2 / totUfidAnnotated),
    num_ucids = numUcids,
    max_rt_stddev_ucid = round(mxRtDevUcid, 2),
    num_ucid2s = numUcid2s,
    max_rt_stddev_ucid2 = round(mxRtDevUcid2, 2),
    names_multiple_ufids = problemNames,
    names_multiple_ufid2s = problemNamesUfid2,
    ufids_multiple_names = problemUfidsNames,
    ufid2s_multiple_names = problemUfid2sNames
  )
}



