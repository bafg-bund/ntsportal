


ufid2_alignment <- function(escon, index = "g2_nts*", rtTol = 0.3, mzTolmDa = 5, minPoints = 5) {
  
  # index <- "g2_nts*"
  # rtTol <- 0.3
  # mzTolmDa <- 5
  # minPoints <- 5


  message("compiling cpp function")


  mzrtpol_dist_FuncPtr <- RcppXPtrUtils::cppXPtr(
    sprintf("
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
  rtPosition <- 0
  numUfids <- 0

  alles <- data.frame(id= character(), ufid2 = numeric())

  repeat {
    res <- elastic::Search(escon, index, body = sprintf('{
      "search_after": [%.4f, %.2f],
      "sort": [
        {
          "mz": {
            "order": "asc"
          }
        },
        {
          "rt": {
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
      }', mzPosition, rtPosition)
    )

    if (length(res$hits$hits) == 0) {
      message("All features were analyzed, no further clusters found")
      message("completed clustering and assigned ", numUfids, " new ufids")
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
    pols <- sapply(pols, digest::digest, algo = "xxhash32")
    pols <- sapply(pols, stringr::str_sub, end = -2)
    pols <- sapply(pols, strtoi, base = 16)
    pols <- sapply(pols, as.double)

    ids <- sapply(res$hits$hits, function(x) x[["_id"]])

    m <- cbind(
      sapply(res$hits$hits, function(x) x[["_source"]]$mz),
      sapply(res$hits$hits, function(x) x[["_source"]]$rt),
      unname(filenames),
      unname(pols)
    )

    message("begin computations")

    # cluster these by mz-rt using parallel distance function
    dist1 <- parallelDist::parDist(m, method="custom", func = mzrtpol_dist_FuncPtr, threads = 6)

    dbscanRes <- dbscan::dbscan(dist1, 0.1, minPoints)

    newcluster <- dbscanRes$cluster
    newcluster[newcluster != 0] <- newcluster[newcluster != 0] + numUfids

    ergebnis <- data.frame(id = ids, ufid2 = dbscanRes$cluster)

    alles <- rbind(alles, ergebnis)

    numUfids <- max(newcluster)

    # set new mz and rt position
    # get last hit
    mzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]] - 2* mzTolmDa / 1000
    rtPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[2]] - 2* rtTol

  }

  

  # write all ufids to Ids

}

# get last hit
# mzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]]
# rtPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[2]]
