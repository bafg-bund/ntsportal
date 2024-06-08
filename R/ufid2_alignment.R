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


#' Alignment by m/z and RT
#'
#'
#' Function will assign ufid2 to features
#'
#' @param escon Connection object created with `elastic::connect` 
#' @param index default is "g2_nts*"
#' @param rtTol retentiontime tolerance in minutes - default is 0.3min
#' @param mzTolmDa mass tolerance in mDa - default is 5mDa
#' @param minPoints minimum number of points to build one cluster
#' @param mzRange For testing purposes, restrict clustering by m/z, numeric length 2 lower and upper limit, default = c(0, 1e6)
#' @param rtRange For testing purposes, restrict clustering by rt, numeric length 2 lower and upper limit, default = c(0, 100)
#' @param numCores Number of cores for parallelization, default 10
#'
#' @return TRUE
#' @export
#' @import logger
#' @import future.apply
ufid2_alignment <- function(escon, index = "g2_nts*", rtTol = 0.3, mzTolmDa = 5,
                            minPoints = 5, mzRange = c(0, 1e6), rtRange = c(0, 100), numCores = 10) {

  stopifnot(is.numeric(mzRange), length(mzRange) == 2)
  stopifnot(is.numeric(rtRange), length(mzRange) == 2)
  
  log_info("Compiling cpp function")
  mzrtpol_dist_FuncPtr <- RcppXPtrUtils::cppXPtr(
    sprintf(
      "
      double customDist(const arma::mat &A, const arma::mat &B) {
        int mzscore;
        int rtscore;
        int filescore;
        int polscore;
        int finalscore;
        double dblfinalscore;

        if (std::islessequal(std::abs(arma::as_scalar(A.col(0) - B.col(0))), %f)) {
         mzscore = 0;
        } else {
         mzscore = 1;
        }

        if (std::islessequal(std::abs(arma::as_scalar(A.col(1) - B.col(1))), %f)) {
         rtscore = 0;
        } else {
         rtscore = 1;
        }

        // check that filenames are different
        int filen1 = arma::as_scalar(A.col(2));
        int filen2 = arma::as_scalar(B.col(2));
        if (filen1 == filen2) {
         filescore = 1;
        } else {
         filescore = 0;
        }

        // check that pols are same
        int pol1 = arma::as_scalar(A.col(3));
        int pol2 = arma::as_scalar(B.col(3));
        if (pol1 == pol2) {
         polscore = 0;
        } else {
         polscore = 1;
        }

        finalscore = mzscore + rtscore + filescore + polscore;
        // std::cout << finalscore << std::endl;
        dblfinalscore = (double)finalscore;  // function must return double
        return dblfinalscore;
      }",
      mzTolmDa / 1000,
      rtTol
    ),
    depends = c("RcppArmadillo")
  )

  log_info("Done.")

  mzPosition <- mzRange[1]
  numUfids <- 0

  alles <- data.frame(id = character(), ufid2 = numeric())

  repeat {
    # if (numUfids > 500)
    #   break
    log_info("Current mz position: {mzPosition}")

    res <- elastic::Search(escon, index, body = sprintf('{
      "search_after": [%.4f],
      "sort": [
        {
          "mz": {
            "order": "asc"
          }
        }
      ],
      "query": {
        "bool": {
          "filter": [
            {
              "exists": {
                "field": "mz"
              }
            },
            {
              "exists": {
                "field": "rt"
              }
            },
            {
              "exists": {
                "field": "rt_clustering"
              }
            },
            {
              "range": {
                "mz": {
                  "gte": %f,
                  "lte": %f
                }
              }
            },
            {
              "range": {
                "rt": {
                  "gte": %f,
                  "lte": %f
                }
              }
            }
          ]
        }
      },
      "size": 10000,
      "_source": ["mz", "rt", "filename", "pol", "rt_clustering"]
      }', mzPosition, mzRange[1], mzRange[2], rtRange[1], rtRange[2]))
    numReturned <- length(res$hits$hits)
    #sameSort <- max(sapply(res$hits$hits, function(x) x[["sort"]][[1]]))
    # Last hit gives new mz position
    newMzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]] - 2 * mzTolmDa / 1000
    if (numReturned <= minPoints || mzPosition == newMzPosition) {
      log_info("All features were analyzed, no further clusters found")
      log_info("Completed clustering and found {numUfids} ufid2s")
      break
    }
    #browser()
    ids <- sapply(res$hits$hits, function(x) x[["_id"]])
    #browser(expr = "Mk67d4UBGz9h5Cn9wRl7" %in% ids && "Hk67d4UBGz9h5Cn9xR4U" %in% ids)
    #which("Mk67d4UBGz9h5Cn9wRl7" == ids)
    #which("Hk67d4UBGz9h5Cn9xR4U" == ids)
    # convert filenames to numeric hash
    filenames <- sapply(res$hits$hits, function(x) x[["_source"]]$filename)
    filenames <- sapply(filenames, digest::digest, algo = "xxhash32")
    filenames <- sapply(filenames, stringr::str_sub, end = -2)
    filenames <- sapply(filenames, strtoi, base = 16)
    filenames <- sapply(filenames, as.double)

    # convert pols to numeric hash
    pols <- sapply(res$hits$hits, function(x) x[["_source"]]$pol)
    pols <- ifelse(pols == "pos", 1.0, 2.0)

    ids <- sapply(res$hits$hits, function(x) x[["_id"]])

    m <- cbind(
      sapply(res$hits$hits, function(x) x[["_source"]]$mz),
      sapply(res$hits$hits, function(x) x[["_source"]]$rt_clustering),
      unname(filenames),
      unname(pols)
    )
    #browser()
    log_info("Clustering...")

    # cluster these by mz-rt using parallel distance function
    dist1 <- parallelDist::parDist(m, method = "custom", func = mzrtpol_dist_FuncPtr, threads = numCores)
    #usedist::dist_get(dist1, 2068, 3414)
    #dbscanRes <- dbscan::dbscan(dist1, 0.1, minPoints)
    #newcluster <- dbscanRes$cluster
    hc <- hclust(dist1, method = "complete")
    newcluster <- cutree(hc, h = 0)
    newcluster[newcluster %in% which(table(newcluster) < minPoints)] <- 0
    newcluster[newcluster != 0] <- newcluster[newcluster != 0] + numUfids

    ergebnis <- data.frame(id = ids, ufid2 = newcluster)
    # subset(ergebnis, id %in% c("Mk67d4UBGz9h5Cn9wRl7", "Hk67d4UBGz9h5Cn9xR4U"))
    alles <- rbind(alles, ergebnis)

    numUfids <- max(newcluster)

    # set new mz position
    mzPosition <- newMzPosition
  }

  # remove all 0 ufids
  alles <- alles[alles$ufid2 != 0, ]

  # check for duplicate features

  compare_ufids <- function(u1, u2) {
    set1 <- alles[alles$ufid2 == u1, "id"]
    set2 <- alles[alles$ufid2 == u2, "id"]

    if (setequal(set1, set2)) {
      return(1L)
      # one is subset of the other
    } else if (all(is.element(set1, set2)) || all(is.element(set2, set1))) {
      return(2L)
      # one is almost a subset of the other
    } else if (length(intersect(set1, set2)) > 0.9 * min(c(length(set1), length(set2)))) {
      return(3L)
      # There is some small amount of overlap
    } else if (length(intersect(set1, set2)) > 0) {
      return(4L)
      # There is no overlap
    } else if (length(intersect(set1, set2)) == 0) {
      return(0L)
      # something else unknown
    } else {
      stop("unknown case with ufid ", u1, " and ", u2)
    }
  }

  if (any(duplicated(alles$id))) {
    logger::log_info("Combining duplicates")
    dupIds <- alles[duplicated(alles$id), "id"]
    dupUfids <- parallel::mclapply(dupIds, function(x) alles[alles$id == x, "ufid2"], mc.cores = numCores)
    dupUfids <- unique(do.call("c", dupUfids))
    plan(multicore, workers = numCores)
    options(future.globals.maxSize = 600e6)
    compMat <- as.matrix(usedist::dist_make(as.matrix(dupUfids), compare_ufids))
    plan(sequential)

    compMat[lower.tri(compMat)] <- 0
    log_info("Completed combining duplicates")
    log_info("Current memory usage {round(pryr::mem_used()/1e6)} MB")
    
    saveRDS(compMat, "compMat.RDS")
    saveRDS(dupUfids, "dupUfids.RDS")
    saveRDS(alles, "alles.RDS")
    # In the case where they are exactly the same, delete one of them
    if (any(compMat == 1)) {
      pairs <- which(compMat == 1, arr.ind = TRUE)
      for (i in seq_len(nrow(pairs))) {
        toCombine <- dupUfids[pairs[i, , drop = TRUE]]
        alles <- alles[alles$ufid2 != toCombine[1], ]
      }
    }
    # In the case where one is the set of another, delete the smaller of the two
    if (any(compMat == 2)) {
      pairs <- which(compMat == 2, arr.ind = TRUE)
      for (i in seq_len(nrow(pairs))) {
        toCombine <- dupUfids[pairs[i, , drop = TRUE]]
        subAlles <- subset(alles, ufid2 %in% toCombine)
        smaller <- which.min(by(subAlles, subAlles$ufid2, nrow))
        alles <- alles[alles$ufid2 != toCombine[smaller], ]
      }
    }
    # In the case where one is almost a subset of the other, add the missing ids to the larger group
    # and delete the smaller group
    if (any(compMat == 3)) {
      pairs <- which(compMat == 3, arr.ind = TRUE)
      for (i in seq_len(nrow(pairs))) {
        toCombine <- dupUfids[pairs[i, , drop = TRUE]]
        subAlles <- subset(alles, ufid2 %in% toCombine)
        if (nrow(subAlles) == 0 || length(unique(subAlles$ufid2)) == 1)
          next
        smaller <- as.numeric(names(which.min(by(subAlles, subAlles$ufid2, nrow))))
        larger <- toCombine[toCombine != smaller]
        idsToChange <- setdiff(
          subset(alles, ufid2 == larger, id, drop = TRUE),
          subset(alles, ufid2 == smaller, id, drop = TRUE)
        )
        stopifnot(length(idsToChange) > 0)
        alles[alles$id %in% idsToChange, "ufid2"] <- larger
        alles <- alles[alles$ufid2 != smaller, ]
      }
    }
    # For any other cases, remove the duplicated ids from both groups but keep the rest as they
    # are, the unclassifiable ids will be left as noise
    if (any(compMat == 4)) {
      pairs <- which(compMat == 4, arr.ind = TRUE)
      for (i in seq_len(nrow(pairs))) {
        toCombine <- dupUfids[pairs[i, , drop = TRUE]]
        subAlles <- subset(alles, ufid2 %in% toCombine)
        subDupIds <- subAlles[duplicated(subAlles$id), "id"]
        alles <- alles[!is.element(alles$id, subDupIds), ]
      }
    }
  }
  
  if (any(duplicated(alles$id))) {
    alles <- alles[!duplicated(alles),]  
  }
  
  if (any(duplicated(alles$id))) {
    stop("Duplicate filter did not work")
  }
  
  # write all ufids to Ids
  log_info("Writing {numUfids} ufid2 to esdb")
  ufidErrors <- numeric()
  for (u in unique(alles$ufid2)) {
    if (u %% 100 == 0)
      logger::log_info("Currently at ufid2 {u}")
    ids <- alles[alles$ufid2 == u, "id", drop = T]
    if (length(ids) == 0)
      next
    tryCatch(
      ntsportal::es_add_ufid_to_ids(
        escon,
        index,
        ufid_to_add = u,
        ids_for_update = ids,
        ufidType = "ufid2"
      ),
      error = function(cnd) {
        logger::log_error("Error adding ufid2 {u}: {conditionMessage(cnd)}")
        # Add this ufid to list of ufids to repeat.
        ufidErrors <<- c(ufidErrors, u)
        # Wait before continuing
        Sys.sleep(10)
      }
    )
  }
  
  if (length(ufidErrors) > 0) {
    logger::log_info("There were {length(ufidErrors)} errors, trying these again")
    for (u in ufidErrors) {
      ids <- alles[alles$ufid2 == u, "id", drop = T]
      if (length(ids) == 0)
        next
      tryCatch(
        ntsportal::es_add_ufid_to_ids(
          escon,
          index,
          ufid_to_add = u,
          ids_for_update = ids,
          ufidType = "ufid2"
        ),
        error = function(cnd) {
          logger::log_error("Error in second attempt at adding ufid2 {u}: {conditionMessage(cnd)}")
          # Just skip it then, wait before continuing
          Sys.sleep(10)
        }
      )
    }
  }

  logger::log_info("Completed ufid2 assignment")
  TRUE
}
