#' Alignment by m/z and RT
#'
#'
#' Function will assign ufid2 to features
#'
#' @param escon
#' @param index default is "g2_nts*"
#' @param rtTol retentiontime tolerance in minutes - default is 0.3min
#' @param mzTolmDa mass tolerance in mDa - default is 5mDa
#' @param minPoints
#'
#' @return TRUE
#' @export
#'
ufid2_alignment <- function(escon, index = "g2_nts*", rtTol = 0.3, mzTolmDa = 5, minPoints = 5) {
  message("Starting ufid2 assignment at ", date())
  message("compiling cpp function")
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

  message("done.")

  mzPosition <- 0
  numUfids <- 0

  alles <- data.frame(id = character(), ufid2 = numeric())

  repeat {
    # if (numUfids > 500)
    #   break
    message("We are at mz ", mzPosition, " at ", date())

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
            }
          ]
        }
      },
      "size": 10000,
      "_source": ["mz", "rt", "filename", "pol"]
      }', mzPosition))
    numReturned <- length(res$hits$hits)
    sameSort <- max(sapply(res$hits$hits, function(x) x[["sort"]][[1]]))
    # last hit gives new mz position
    newMzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]] - 2 * mzTolmDa / 1000
    if (numReturned <= minPoints || mzPosition == newMzPosition) {
      message("All features were analyzed, no further clusters found")
      message("completed clustering and found ", numUfids, " ufid2s")
      break
    }

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
      sapply(res$hits$hits, function(x) x[["_source"]]$rt),
      unname(filenames),
      unname(pols)
    )

    message("Clustering...")

    # cluster these by mz-rt using parallel distance function
    dist1 <- parallelDist::parDist(m, method = "custom", func = mzrtpol_dist_FuncPtr, threads = 6)

    dbscanRes <- dbscan::dbscan(dist1, 0.1, minPoints)

    newcluster <- dbscanRes$cluster
    newcluster[newcluster != 0] <- newcluster[newcluster != 0] + numUfids

    ergebnis <- data.frame(id = ids, ufid2 = newcluster)

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
    message("Combining duplicates at ", date())
    dupIds <- alles[duplicated(alles$id), "id"]
    dupUfids <- parallel::mclapply(dupIds, function(x) alles[alles$id == x, "ufid2"], mc.cores = 10)
    dupUfids <- unique(do.call("c", dupUfids))

    compMat <- as.matrix(ntsportal::dist_make_parallel(as.matrix(dupUfids), compare_ufids, numCores = 10))

    compMat[lower.tri(compMat)] <- 0
    # in the case where they are exactly the same, delete one of them
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
        smaller <- which.min(by(subAlles, subAlles$ufid2, nrow))
        larger <- which.max(by(subAlles, subAlles$ufid2, nrow))
        idsToChange <- setdiff(
          subset(alles, ufid2 == toCombine[smaller], id, drop = TRUE),
          subset(alles, ufid2 == toCombine[larger], id, drop = TRUE)
        )
        stopifnot(length(idsToChange) > 0)
        alles[alles$id %in% idsToChange, "ufid2"] <- toCombine[larger]
        alles <- alles[alles$ufid2 != toCombine[smaller], ]
      }
    }
    # for any other cases, remove the duplicated ids from both groups but keep the rest as they
    # are, the unclassifiable ids will be left as noise
    if (any(compMat == 4)) {
      stop("case 4 has not been tested yet")
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
    stop("Duplicate filter did not work")
  }

  # write all ufids to Ids
  message("Writing ", numUfids, " ufid2 to esdb at ", date())

  for (u in unique(alles$ufid2)) {
    message("Updating ", u, " at ", date())
    ntsportal::es_add_ufid2_to_ids(
      escon,
      index,
      ufid2_to_add = u,
      ids_for_update = alles[alles$ufid2 == u, "id", drop = T]
    )
  }

  message("Completed ufid2 assignment at ", date())
  TRUE
}
