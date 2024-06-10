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




# Functions to analyze ES DB for testing purposes

#' Compute silhouette score for all ufids in the index
#'
#' @param escon Connection object created with `elastic::connect`
#' @param testIndex Elasticsearch index name 
#' @param ufidType Either "ufid" or "ufid2"
#'
#' @return dataframe with the scores for each feature
#' @export
#'
silhouette_score <- function(escon, testIndex, ufidType = "ufid") {
  # Section to define Functions ####

  fast_mean_distance <- function(x){
    n <- length(x)
    i <- rep(1:n,each = n)
    j <- rep(1:n,n)
    dxn <- abs(x[j]-x[i])
    dxn2 <- cumsum(dxn)
    dxn2 <- c(0,dxn2[seq(from=n,to=length(dxn),by=n)])
    dxn3 <- dxn2[-1]-dxn2[-length(dxn2)]
    dxn3 <- dxn3/(n-1)
    return(dxn3)
  }
  
  Qscore_binning <- function(mz,ID,Scans,rt_tolerance){
    
    ## Function to calculate DQSbin in analogy to the silhouette score
    if(length(unique(ID))==1){return(rep(NA,length(mz)))}
    filter_ID <- ID>0
    
    rt_binl <- split(Scans[filter_ID],f = ID[filter_ID])
    mz_binl <- split(mz[filter_ID],f = ID[filter_ID])
    row_binl <- 1:length(mz)
    row_n_dt <- row_binl*0
    row_binl <- split(row_binl[filter_ID],f = ID[filter_ID])
    len_rowbinl <- as.numeric(lapply(rt_binl,length))
    matrix2l <- lapply(rt_binl,FUN= function(x){matrix(rep(x,2),ncol = 2)})
    mz_mat1l <- lapply(mz_binl, FUN = function(x){matrix(rep(x,2),ncol=2)})
    matrix2vl <- lapply(matrix2l,FUN=function(x){as.numeric(x)})
    checkupl <- lapply(matrix2vl, FUN = function(x){ (1:(length(x)/2))})
    mean_dist <- rep(0,length(mz))
    
    for(bin in 1:length(rt_binl)){
      binOpen <- T
      rt_bin <- rt_binl[[bin]]
      row_bin <- row_binl[[bin]]
      matrix2 <- matrix2l[[bin]]
      matrix2v <- matrix2vl[[bin]]
      mz_bin <- mz_binl[[bin]]
      mz_mat1 <- mz_mat1l[[bin]]
      len_rowbin <- len_rowbinl[bin]
      checkup <- checkupl[[bin]]
      
      k <- 1
      row_next <- c(row_bin[1],row_bin[len_rowbin])
      mat_row <- length(mz)
      
      mean_dist[row_bin] <- fast_mean_distance(mz_bin)
      
      while(binOpen){
        
        row_next <- row_next + c(-1,1)
        row_next[row_next<1] <- 1
        row_next[row_next>mat_row] <- mat_row
        rt_next <- c(Scans[row_next[1]],Scans [row_next[2]])
        
        rt_mat <-  abs(rep(rt_next,each=len_rowbin)-matrix2v)<=rt_tolerance
        
        if(sum(rt_mat[c(checkup,checkup+len_rowbin)]) == 0){
          k <- k+1
        }else{
          
          rt_mat <-  matrix(rt_mat,ncol=2)
          xor_logic <- xor(rt_mat[,1],rt_mat[,2])
          row_neighbor <- (rt_mat*xor_logic)%*%as.matrix(row_next)
          
          nb <- (rt_mat[,1]+rt_mat[,2])==2
          if(sum(nb)>0){
            mz_next <- c(mz[row_next[1]],mz[row_next[2]])
            mz_mat2 <- matrix(data =  rep(mz_next,each=len_rowbin),ncol = 2)
            mz_mat3 <- abs(mz_mat1 - mz_mat2)
            row_neighbor[nb] <- ifelse(mz_mat3[,1]<mz_mat3[,2],row_next[1],row_next[2])[nb]
          }
          
          checkup <- which(row_n_dt[row_bin] ==0)
          
          row_n_dt[row_bin][checkup] <- as.numeric(row_neighbor)[checkup]
          
          
          
          
          if(length(checkup)==0){binOpen <- F}
          k <- k+1
        }
        
        
        
        
      }
      
    }
    
    mz_f <- mz[filter_ID]
    mz_p <- mz[row_n_dt]
    mz_d <- abs(mz_f-mz_p)
    mean_dist_f <- mean_dist[filter_ID]
    A <- 1/(1+mean_dist[filter_ID])
    si <- ((mz_d-mean_dist[filter_ID])/(ifelse(mz_d>mean_dist_f,mz_d,mean_dist_f)))*A
    si <- (si+1)/2
    return(si)
  }
  
  
  
  # Collect data from ntsp ####
  
  
  startAfterUfid <- 0
  df <- data.frame(mz = numeric(), rt = numeric(), ID = numeric())
  repeat {
    Testresult <- elastic::Search(
      escon,
      testIndex,
      body = sprintf('
  {
   "search_after": [%i],
    "query" : {
      "exists": {
        "field": "%s"
      }
    },
    "sort": [
      {
        "%s": {
          "order": "asc"
        }
      }
    ],
    "_source": ["mz", "rt_clustering", "%s"],
    "size": 10000
  }

  ', startAfterUfid, ufidType, ufidType, ufidType)
    )
    hitsLeft <- length(Testresult$hits$hits)
    if (hitsLeft == 0)
      break
    
    startAfterUfid <- Testresult$hits$hits[[hitsLeft]]$sort[[1]]
    
    ## Extract the relevant information for cluster (mz, rt, cluster ID)
    mz <- c()
    rt <- c()
    ID <- c()
    for(k in 1:length(Testresult$hits$hits)) {
      mz[k] <- Testresult$hits$hits[[k]][["_source"]]$mz
      rt[k] <- Testresult$hits$hits[[k]][["_source"]]$rt_clustering
      if (is.null(Testresult$hits$hits[[k]][["_source"]][[ufidType]])) {
        ID[k] <- NA
      } else {
        ID[k] <- Testresult$hits$hits[[k]][["_source"]][[ufidType]]  
      }
    }
    tempdf <- data.frame(mz=mz,rt=rt,ID=ID)
    df <- rbind(df, tempdf)
  }
  
  # Process data ####
  
  # Rescale the rt into scans starting from 1 at the first retention time
  df <- df[order(df$rt,decreasing = F),]
  df$Scans <- c(0)
  df$Scans <- c(1,diff(df$rt)>0)
  df$Scans <- cumsum(df$Scans )
  
  # Rescale the IDs so the first ID is 1.
  df <- df[order(df$ID,decreasing = F),]
  df$ID <- c(1,diff(df$ID)>0)
  df$ID <- cumsum(df$ID)
  # Sort the dataframe by m/z as it is necessary for the scoring algorithm to be efficient
  df <- df[order(df$mz,decreasing = F),]
  # Algorithm cannot deal with non-unique m/z value. Thus, we add a random value 
  # on top only for scoring algorithm. This should not change the m/z values significantly.
  df$mz <- df$mz + runif(n=length(df$mz),min = 0.0001,max=0.001)
  # Application of the function to obtain the silhouette scores.
  
  drt <- mean(diff(sort(unique(rt))))
  rt_tol_seconds <- 30
  rt_tol_scans <- rt_tol_seconds/drt
  
  df$QScore <- Qscore_binning(mz = df$mz,ID = df$ID,Scans = df$Scans,rt_tolerance = rt_tol_scans)
  # Calculate size of the clusters
  df <- df %>% group_by(ID) %>% mutate(ID2 = ID*runif(1),len_ID = length(mz))
  
  # Silhouette Scores for clusters of size 1 are not defined from a mathematical 
  # point of view as they have no within cluster variance. Thus, their score is set to 0.
  df$QScore[df$len_ID==1] <- 0
  df
}



#' Compute validation statistics for ufid and ucid assignment
#'
#' Used for testing purposes.
#'
#' @param escon Connection object created with `elastic::connect`
#' @param index Elasticsearch index name 
#' @param includeUfid2 include ufid2 stats
#' @param includeUcid include ucid stats
#'
#' @return list of metrics used to assess quality of ufid and ucid assignments
#' @export
es_test_fpfn <- function(escon, index, includeUfid2 = FALSE, includeUcid = FALSE) {
  logger::log_info("Starting testing on index {index}")
  # index <- "g2_nts_v3_upb"
  # source("~/connect-ntsp.R")
   
  # Get total number of features with different constraints
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

  # Number of features with ufid
  
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
  totWithUfid <- tot_with_ufid("ufid", FALSE)
  
  if (includeUfid2) {
    percentWithUfid2 <- tot_with_ufid("ufid2", TRUE)
    totWithUfid2 <- tot_with_ufid("ufid2", FALSE)
  }
  
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
  
  # Number of ufids, average cluster size
  ufidStats <- elastic::Search(escon, index, size = 0, body = '
  {
      "query": {
        "match_all": {}
      },
      "aggs": {
        "stats_ufids" : {
          "extended_stats_bucket": {
            "buckets_path": "ufids._count"
          }
        },
        "ufids": {
          "terms": {
            "field": "ufid",
            "size": 100000
          }
        }
      }
    }                
    ')
  
  if (ufidStats$aggregations$ufids$sum_other_doc_count != 0)
    logger::log_warn("When counting ufids, not all ufids could be sorted into buckets")
  totNumUfids <- ufidStats$aggregations$stats_ufids$count
  avgNumFeat <- ufidStats$aggregations$stats_ufids$avg
  
  # Compute gap-filling for ufid (features with no ms2)
  numGapFill <- elastic::Search(escon, index, size = 0, body = '
                                {
  "query": {
    "bool": {
      "must": [
        {
          "exists": {
            "field": "ufid"
          }
        }
      ],
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
        }
      ]
    }
  }
}
  ')$hits$total$value
  
  
  
  # Computation of fp ####
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
  if (includeUfid2)
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
  if (includeUfid2)
    totFPFeatures1ufid2 <- tot_fp_features_1(problemUfid2s, "ufid2")
  
  problem_ufids_names <- function(probUfids, ufidType) {
    if (length(probUfids) == 0)
      return(NULL)
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
  if (includeUfid2)
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
  if (includeUfid2) {
    problemNamesUfid2 <- c(type2FpPosUfid2$problemNames, type2FpNegUfid2$problemNames)
    totFPFeatures2Ufid2 <- sum(type2FpPosUfid2$totFPFeatures2, type2FpNegUfid2$totFPFeatures2)
  }

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
    tot <- sum(vapply(b, function(x) x$doc_count, numeric(1)) - 1)
    #browser(expr = tot > 0 && ufidType == "ufid2")
    tot
  }
  logger::log_info("Computing duplicated ufid assignments to one sample")
  numberMultiAssignUfid <- sum(
    vapply(
      allUfids, 
      get_docs_multi_assignment, 
      numeric(1), 
      ufidType = "ufid"
    )
  )
  
  if (includeUfid2) {
    logger::log_info("Computing duplicated ufid2 assignments to one sample")
    numberMultiAssignUfid2 <- sum(
      vapply(
        allUfid2s, 
        get_docs_multi_assignment, 
        numeric(1), 
        ufidType = "ufid2"
      )
    )
  }
  
  
  
  # ucid accuracy ####
  
  if (includeUcid) {
    # compute max sd in rt across all ucids
    logger::log_info("Computing ucid accuracy")
    
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
    if (includeUfid2) {
      ucid2List <- test_ucid("ucid2")
      mxRtDevUcid2 <- ucid2List$mxRtDevUcid
      numUcid2s <- ucid2List$numUcids
    }
  }

  logger::log_info("Completed tests")
  # Output List ####
  output <- list(
    total_features = totFeat,
    total_features_with_ufid = totWithUfid,
    total_num_ufids = totNumUfids,
    average_features_per_ufid = round(avgNumFeat),
    percent_fn_ufid = 100 - round(percentWithUfid),
    total_features_with_ms2 = totFeatMs2,
    percent_with_ms2 = round(ms2percent),
    total_from_gapfill = numGapFill,
    percent_from_gapfill = round(100 * numGapFill / totWithUfid),
    percent_ms2_fn_ufid = 100 - round(percentMs2withUfid),
    percent_ufids_multiple_assignment = round(100 * numberMultiAssignUfid / totWithUfid),
    total_features_ufid_annotated = totUfidAnnotated,
    percent_fp_multiple_names_by_ufid = round(100 * totFPFeatures1ufid / totUfidAnnotated),
    percent_fp_multiple_ufids_by_name = round(100 * totFPFeatures2 / totUfidAnnotated),
    names_multiple_ufids = problemNames,
    ufids_multiple_names = problemUfidsNames
  )
  
  if (includeUfid2) {
    output2 <- list(
      percent_fn_ufid2 = 100 - round(percentWithUfid2),
      percent_ufid2s_multiple_assignment = round(100 * numberMultiAssignUfid2 / totWithUfid2),
      total_features_ufid2_annotated = totUfid2Annotated,
      percent_fp_multiple_names_by_ufid2 = round(100 * totFPFeatures1ufid2 / totUfid2Annotated),
      percent_fp_multiple_ufid2s_by_name = round(100 * totFPFeatures2Ufid2 / totUfidAnnotated),
      names_multiple_ufid2s = problemNamesUfid2,
      ufid2s_multiple_names = problemUfid2sNames
    )
    output <- c(output, output2)
  }
  
  if (includeUcid && !includeUfid2) {
    output3 <- list(
      num_ucids = numUcids,
      max_rt_stddev_ucid = round(mxRtDevUcid, 2)
    )
    output <- c(output, output3)
  }
  
  if (includeUcid && includeUfid2) {
    output4 <- list(
      num_ucid2s = numUcid2s,
      max_rt_stddev_ucid2 = round(mxRtDevUcid2, 2)
    )
    output <- c(output, output4)
  }
  output
}


#' Produce plots showing mz, rt stats of ufids in an index
#'
#' @param escon Connection object created with `elastic::connect`
#' @param testIndex Index name
#' @param ufidType Either "ufid" or "ufid2"
#'
#' @return Plots of m/z, retention time and sillouhette score.
#' @export
mz_rt_stats_plots <- function(escon, testIndex, hideTitleX = FALSE, ufidType = "ufid") {
  
  # Collect aggregation data
  res <- elastic::Search(escon, testIndex, body = sprintf('
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
          "size": 100000
        },
        "aggs": {
          "mz_stats": {
            "extended_stats": {
              "field": "mz"
            }
          },
          "rt_stats": {
            "extended_stats": {
              "field": "rt_clustering"
            }
          }
        }
      }
    }
  }
  ', ufidType, ufidType))
  
  ufs <- res$aggregations$ufids$buckets
  stds <- data.frame(
    ufid = sapply(ufs, "[[", i = "key"),
    mz_stddev = sapply(ufs, function(x) x$mz_stats$std_deviation) * 1000,
    rt_stddev = sapply(ufs, function(x) x$rt_stats$std_deviation)
  )
  
  #paste(subset(stds, mz_stddev > 8, "ufid", drop = T), collapse = ", ")
  
  mzq <- quantile(stds$mz_stddev, 0.95)
  plotMz <- ggplot(stds, aes(mz_stddev)) + geom_density()
  curv <- curv <- ggplot_build(plotMz)$data[[1]]  
  plotMz <- plotMz +
    geom_area(aes(x, y), subset(curv, x < mzq), fill = "grey30", alpha = .5) +
    geom_vline(aes(xintercept = mzq), linetype = 2) +
    xlab("Std. Dev. m/z (mDa)") +
    ylab("Density") +
    theme_bw(9) +
    scale_x_continuous(limits = c(0, 4)) +
    theme(
      axis.text.y = element_blank(),
      panel.grid.major = element_blank(), 
      panel.grid.minor = element_blank()
      )
  
  rtq <- quantile(stds$rt_stddev, 0.95)
  plotRt <- ggplot(stds, aes(rt_stddev)) + geom_density()
  curv2 <- ggplot_build(plotRt)$data[[1]]
  plotRt <- plotRt +
    geom_area(aes(x, y), subset(curv2, x < rtq), fill = "grey30", alpha = .5) +
    geom_vline(aes(xintercept = rtq), linetype = 2) +
    xlab(expression(paste("Std. Dev. ", t[R] , " (min)"))) +
    ylab("Density") +
    theme_bw(9) + 
    scale_x_continuous(limits = c(0, 1)) +
    theme(
      axis.text.y = element_blank(),
      axis.title.y = element_blank(),
      panel.grid.major = element_blank(), 
      panel.grid.minor = element_blank()
    )
  df <- silhouette_score(escon, testIndex, ufidType = ufidType)
  
  gt <- round(100*nrow(subset(df, QScore > .5))/nrow(df))
  
  silPlot <- ggplot(df, aes(QScore)) + 
    geom_density() +
    theme_bw(9) +
    annotate("text", x = -Inf, y = Inf, 
             label = glue::glue("{gt}% QScore > .5"), hjust = -.05, vjust = 1.3) +
    theme(
      axis.text.y = element_blank(),
      axis.title.y = element_blank(),
      panel.grid.major = element_blank(), 
      panel.grid.minor = element_blank()
    )
  
  if (hideTitleX) {
    plotMz <- plotMz +
      theme(axis.title.x = element_blank())
    plotRt <- plotRt +
      theme(axis.title.x = element_blank())
    silPlot <- silPlot +
      theme(axis.title.x = element_blank())
  }
  
  list(plotMz = plotMz, plotRt = plotRt, silPlot = silPlot)
}

