


#' Add new ufid entry by dbscan clustering
#'
#' This is the main function for the clustering stage of NTSP alignment.
#' New features, which do not yet have a ufid entry, are clustered based on 
#' their similarity (within tolerances) in the m/z, tR and MS2 domains. New 
#' ufids are added both to NTSP and ufid-db.
#'
#' @param ubd ufid database connection object (DBI::dbConnect) 
#' @param escon NTSPortal connection object (elastic::connect)
#' @param index Index name
#' @param polarity polarity of features to process
#' @param minPoints1 minimum number of features for first stage clustering
#' @param minPoints2 minimum number of features for second stage clustering
#' @param mzPosition minimum mz (Da) for returning new features from NTSPortal
#' @param rtPosition minimum RT (min) for returning new features from NTSPortal
#'
#' @return TRUE if the function ended successfully
#' @export
#' 
#' @import RcppXPtrUtils
#' @import RcppArmadillo
#' @import dplyr
#' @import logger
#' @import glue
ubd_new_ufid <- function(ubd, escon, index, polarity, minPoints1 = 5, 
                         minPoints2 = 2, mzPosition = 0, rtPosition = 0) {
  # polarity <- "pos"
  # library(RcppXPtrUtils)
  # library(RcppArmadillo)
  # library(dplyr)
  # library(logger)
  # library(glue)
  # minPoints1 = 5
  # minPoints2 = 2
  # setwd("tests/ufid_alignment")
  
  # set up c++ function ####
  log_info("----- Starting clustering with udb_new_ufid -----")
  log_info("Current memory usage {round(pryr::mem_used()/1e6)} MB")
  if (!is.numeric(config::get("ms2_ndp_min_score"))) 
    stop("config.yml file not found or settings missing")
  
  logger::log_info("compiling cpp function")
  
  # Define c++ function for 1st stage clustering
  mzrt_dist_FuncPtr <- RcppXPtrUtils::cppXPtr(
    sprintf("
      double customDist(const arma::mat &A, const arma::mat &B) {
        int mzscore;
        int rtscore;
        int filescore;
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

        finalscore = mzscore + rtscore + filescore;
        // std::cout << finalscore << std::endl;
        dblfinalscore = (double)finalscore;  // function must return double
        return dblfinalscore;
      }",
            config::get("mztol_clustering_mda") / 1000,
            config::get("rttol_clustering_min")
    ),
    depends = c("RcppArmadillo")
  )
  
  log_info("Compilation complete")
  
  # other private functions ####
  
  calc_ndp_purity <- function(d_spec, db_spec, ndp_m = 2, ndp_n = 1, mztolu_ = 0.015) {
    ar <- which(abs(outer(d_spec[, 1], db_spec[, 1], "-")) <= mztolu_,
                arr.ind = TRUE)  # find matching masses
    if (nrow(ar) == 0)
      return(0)
    
    m <- cbind(d_spec[, 1][ar[, 1]], db_spec[, 1][ar[, 2]])  # extract matching
    
    d_int <- d_spec[, 2][ar[, 1]]
    db_int <- db_spec[, 2][ar[, 2]]
    masses <- rowMeans(m)
    # extract non-matching
    d_nonmatching <- d_spec[-ar[,1],]
    db_nonmatching <- db_spec[-ar[,2],]
    
    d_int <- append(d_int, d_nonmatching[, 2])
    masses <- append(masses, d_nonmatching[, 1])
    db_int <- append(db_int, rep(0, nrow(d_nonmatching)))
    
    db_int <- append(db_int, db_nonmatching[, 2])
    masses <- append(masses, db_nonmatching[, 1])
    d_int <- append(d_int, rep(0, nrow(db_nonmatching)))
    
    WS1 <- d_int^ndp_m * masses^ndp_n
    WS2 <- db_int^ndp_m * masses^ndp_n
    
    r_ndp <- (sum(WS1 * WS2))^2/(sum(WS1^2) * sum(WS2^2))
    
    # if either of the two spectra only have 1 fragment, then the most intense
    # fragement in both spectra must be the same mass, otherwise return 0
    if (nrow(d_spec) == 1 || nrow(db_spec) == 1) {
      mzD <- d_spec[which.max(d_spec[, 2]), 1]
      mzS <- db_spec[which.max(db_spec[, 2]), 1]
      if (abs(mzD - mzS) > mztolu_)
        r_ndp <- 0
    }
    
    r_ndp * 1000
  }
  
  feature_compare <- function(ftx, fty, mztol = 0.005, rttol = 0.5,
                              ms2mztol = 0.03, ms2dpthresh = 400, ndp_m = 2, ndp_n = 1) {
    inMz <- ifelse(abs(ftx$mz - fty$mz) <= mztol, TRUE, FALSE)
    inRt <- ifelse(abs(ftx$rt_clustering - fty$rt_clustering) <= rttol, TRUE, FALSE)
    inMS2 <- ifelse(
      calc_ndp_purity(ftx$ms2, fty$ms2, ndp_m, ndp_n, ms2mztol) >= ms2dpthresh, TRUE, FALSE
    )
    all(inMz, inRt, inMS2)
  }
  
  custom_distance <- function(id1, id2, mztol = 0.005, rttol = 0.5,
                              ms2mztol = 0.03, ms2dpthresh = 400, ndp_m = 2, ndp_n = 1) {
    
    ft1 <- listoFeatures[[id1]]
    ft2 <- listoFeatures[[id2]]
    
    ifelse(feature_compare(ft1, ft2, mztol = mztol, rttol = rttol,
                           ms2mztol = ms2mztol, ms2dpthresh = ms2dpthresh, ndp_m = ndp_m, ndp_n = ndp_n), 0, 1)
  }
  
  # begin computations ####
  
  # get set of 10000 features from the ntsportal, sort by mz and rt so that you
  # are generally accessing the same domain and not taking randomly from everywhere
  # one possible issue: you keep accessing the same docs if they never get a ufid
  # to solve this do a loop with the "search_after" parameter which keeps
  # accessing more pages until a cluster is found or no more results are returned
  
  # polarity <- "pos"
  log_info("Begin loop through all unassigned features")
  # mzPosition <- 255
  # rtPosition <- 5
  # mzPosition <- 171
  # rtPosition <- 2.3
  # use rt_clustering field instead of rt
  # rt_clustering is at the moment the rt of the bfg_nts_rp1 method (predicted or experimental)
  # copied from the rtt table to the top level using add-rt_clustering-field.R
  
  numUfids <- 0
  repeat {
    log_info("Access database from position m/z {mzPosition}, rt {rtPosition}, pol {polarity}")
    skipToNext <- FALSE
    tryCatch(
      res <- elastic::Search(escon, index, body = sprintf('{
      "search_after": [%.4f, %.2f],
      "sort": [
        {
          "mz": {
            "order": "asc"
          }
        },
        {
          "rt_clustering": {
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
              "term": {
                "pol": "%s"
              }
            },
            {
              "exists": {
                "field": "rt_clustering"
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
          ],
          "must_not": [
           {
              "exists": {
                "field": "ufid"
              }
            }
          ]
        }
      },
      "size": 10000,
      "_source": ["mz", "rt_clustering", "filename"]
      }', mzPosition, rtPosition, polarity)),
      error = function(cnd) {
        log_error("Error text: {conditionMessage(cnd)}")
        log_info("Error retrieving new docs from ntsp, try again in 1 h")
        Sys.sleep(3600)
        skipToNext <<- TRUE
      }
    )
    if (skipToNext) {
      log_warn("Trying to retrieve documents again")
      next
    }
    
    log_info("In total, {res$hits$total$value} features left to process")
    
    # If nothing is returned by the database, you have reached the end.
    # this is where the repeat breaks and the function ends
    if (length(res$hits$hits) == 0) {
      log_info("All features were analyzed, no further clusters found")
      log_info("Current memory usage {round(pryr::mem_used()/1e6)} MB")
      log_info("Completed clustering and assigned {numUfids} new ufids")
      return(TRUE)
    }
    
    log_info("Beginning computations on the next {length(res$hits$hits)} features")
    
    # convert filenames to numeric hash so that it can be used by armadillo
    filenames <- sapply(res$hits$hits, function(x) x[["_source"]]$filename)
    filenames <- sapply(filenames, digest::digest, algo = "xxhash32")
    filenames <- sapply(filenames, stringr::str_sub, end = -2)
    filenames <- sapply(filenames, strtoi, base = 16)
    filenames <- sapply(filenames, as.double)
    ids <- sapply(res$hits$hits, function(x) x[["_id"]])
    
    m <- cbind(
      sapply(res$hits$hits, function(x) x[["_source"]]$mz),
      sapply(res$hits$hits, function(x) x[["_source"]]$rt_clustering),
      unname(filenames)
    )
    
    # 1st stage clustering ####
    # cluster these by mz-rt using parallel distance function
    dist1 <- parallelDist::parDist(m, method="custom", func = mzrt_dist_FuncPtr)
    #table(r1)
    
    dbscanRes <- dbscan::dbscan(dist1, 0.1, minPoints1)
    
    if (length(names(table(dbscanRes$cluster))) > 1) {
      tblc <- table(dbscanRes$cluster)
      viableTblc <- tblc[names(tblc) != "0"]
      log_info("{length(viableTblc)} cluster(s) with {minPoints1} or more features found")
      distrib <- hist(viableTblc, plot = F)
      message("Histogram:")
      message(glue("Breaks: {paste(distrib$breaks[-1], collapse = ' ')}"))
      message(glue("Counts: {paste(distrib$counts, collapse = ' ')}"))
      log_info("Current memory usage {round(pryr::mem_used()/1e6)} MB")
      # go through all viable clusters and perform second stage clustering
      # with ms1 and ms2
      clusters <- data.frame(id = ids, cluster = dbscanRes$cluster)
      clusters <- clusters[clusters$cluster != 0, ]
      # go through all these clusters with the largest first
      tblcdf <- as.data.frame(tblc, stringsAsFactors = F)
      tblcdf <- tblcdf[tblcdf$Var1 != "0",]
      tblcdf <- tblcdf[order(tblcdf$Freq, decreasing = T),]
      
      for (cl in as.numeric(tblcdf$Var1)) {  # cl <- 17
        # select the cluster
        ids_of_compound <- clusters[clusters$cluster == cl, "id"]
        if (length(ids_of_compound) < minPoints2) {
          log_info("Not enough docs found for cluster {cl}, moving to next")
          next
        }
        log_info("Second stage clustering with {length(ids_of_compound)} features")
        
        # cluster these candidates again by mz-rt-ms2 with non-parallel distance function
        skipToNext <- FALSE
        tryCatch(
          {
            listoFeatures <- lapply(ids_of_compound, function(i) {
              if (ntsportal::es_check_feat(escon, index, i)) {
                suppressMessages(ntsportal::es_feat_from_id(escon, index, i))
              } else {
                NULL
              } 
            })
            names(listoFeatures) <- ids_of_compound
            listoFeatures <- Filter(Negate(is.null), listoFeatures) 
          },
          error = function(cnd) {
            log_error("Error retrieving features from IDs")
            log_info("Current m/z position: {mzPosition}")
            log_info("Current tR position: {rtPosition}")
            log_info("Current 1st stage cluster: {cl}")
            skipToNext <<- TRUE
            log_info("Error text: {conditionMessage(cnd)}")
            ntsportal::es_error_handler(cnd)
          }
        )
        if (skipToNext) {
          log_warn("Skipping to next 1st stage cluster")
          next
        }
        
        # 2nd stage clustering ####
        dist2 <- usedist::dist_make(
          as.matrix(names(listoFeatures)),
          custom_distance,
          mztol = config::get("mztol_clustering_mda") / 1000,
          rttol = config::get("rttol_clustering_min"),
          ms2mztol = config::get("mztol_ms2_ndp_clustering_mda") / 1000,
          ms2dpthresh = config::get("ms2_ndp_min_score_clustering"),
          ndp_m = config::get("ms2_ndp_m"),
          ndp_n = config::get("ms2_ndp_n")
        )
        
        dbscanRes2 <- dbscan::dbscan(dist2, 0.1, minPoints2)
        
        if (length(names(table(dbscanRes2$cluster))) == 1 &&
            names(table(dbscanRes2$cluster)) == "0") {
          message("no 2nd cluster with ", minPoints2 ,"or more features found, moving to next
                  viable 1st stage cluster")
        } else {
          tblc2 <- table(dbscanRes2$cluster)
          viableTblc2 <- tblc2[names(tblc2) != "0"]
          log_info("Found {length(viableTblc2)} cluster(s) with {minPoints2} or more features after 2nd stage clustering")
          distrib2 <- hist(viableTblc2, plot = F)
          message("Histogram:")
          message(glue("Breaks: {paste(distrib2$breaks[-1], collapse = ' ')}"))
          message(glue("Counts: {paste(distrib2$counts, collapse = ' ')}"))
          # take the cluster with the highest number first, what about the other 
          # second stage clusters?
          # At the moment these are just forgotten 
          # TODO additional for loop needed to add each group of second stage clustering
          clusters2 <- data.frame(id = names(listoFeatures), cluster = dbscanRes2$cluster)
          ids_of_compound2 <- clusters2[clusters2$cluster == which.max(tblc2[names(tblc2) != "0"]), "id"]
          
          res2 <- elastic::docs_mget(escon, index, ids = ids_of_compound2, verbose = F)
          # if docs have a name, add this to ufid db as well
          named <- vapply(res2$docs, function(x) is.element("name", names(x[["_source"]])), logical(1))
          # if more than half have a name, add these to ufid DB
          if (sum(named) > length(named) * 0.5) {
            comps <- vapply(res2$docs, function(x) paste(x[["_source"]][["name"]], collapse = ", "), character(1))
            comps <- comps[comps != ""]
            comp_name <- paste(unique(comps), collapse = ", ")
            log_info("Cluster is known compound(s) {comp_name}")
          } else {
            log_info("Cluster is not annotated")
            if (exists("comp_name"))
              rm(comp_name)
            mzs <- vapply(res2$docs, function(x) x[["_source"]][["mz"]], numeric(1))
            rts <- vapply(res2$docs, function(x) x[["_source"]][["rt"]], numeric(1))
            message(sprintf("m/z: %.4f, stdev: %.4f; rt: %.2f, stdev: %.2f", mean(mzs), sd(mzs), mean(rts), sd(rts)))
          }
          
          polarities <- sapply(res2$docs, function(x) x[["_source"]]$pol)
          stopifnot(length(unique(polarities)) == 1)
          
          # Assign new ufid to these ids, then build new ufid entry in ufid_db
          new_ufid <- ntsportal::get_next_ufid(udb, escon, index)
          
          log_info("Adding ufid {new_ufid} to docs in ntsp")
          skipToNext <- FALSE
          tryCatch(
            ntsportal::es_add_ufid_to_ids(escon, index, new_ufid, ids_of_compound2),
            error = function(cnd) {
              log_error("Error in es_add_ufid_to_ids for ufid {new_ufid}")
              log_info("In total, {length(ids_of_compound2)} docs were to be updated")
              log_info("Current mzPosition: {mzPosition}")
              log_info("Current rtPosition: {rtPosition}")
              log_info("Current 1st stage cluster: {cl}")
              log_info("Error text: {conditionMessage(cnd)}")
              skipToNext <<- TRUE
            }
          )
          if (skipToNext) {
            # Remove any partial ufid assignments
            ntsportal::es_remove_ufid(escon, index, new_ufid)
            log_warn("Skipping to next 1st stage cluster")
            next
          }
          
          # Wait a few seconds (it was theorized that errors in the next step
          # are occurring because ntsp is not ready yet)
          Sys.sleep(5)
          
          log_info("Updating ufid-db with averaged data from NTSP")
          # here we create a new ufid, so we are only interested in the current
          # index (no need to use generic g2_nts* index).
          skiptToNext <- FALSE
          tryCatch(
            udbUpdated <- ntsportal::udb_update(udb, escon, index, new_ufid),
            error = function(cnd) {
              log_error("Error in udb_update for ufid {new_ufid}")
              log_info("Current mzPosition: {mzPosition}")
              log_info("Current rtPosition: {rtPosition}")
              log_info("Current 1st stage cluster: {cl}")
              log_info("Error text: {conditionMessage(cnd)}")
              skiptToNext <<- TRUE
              Sys.sleep(5)
              log_warn("Rolling back changes to ntsp")
              try(es_remove_ufid(escon, index, new_ufid))
            }
          )
          if (skipToNext) {
            ntsportal::es_remove_ufid(escon, index, new_ufid)
            ntsportal::udb_remove_ufid(udb, new_ufid)
            log_warn("Skipping to next 1st stage cluster")
            next
          }
          if (udbUpdated) {
            log_info("new ufid {new_ufid} successfully added to ufid DB")
          }
          # add name to ufid-db
          if (sum(named) > length(named) * 0.5 && exists("comp_name")) {
            try(DBI::dbExecute(udb, sprintf('
               UPDATE feature
               SET compound_name = "%s"
               WHERE
                 ufid == %i;
                 ', comp_name, new_ufid)))
          }
          # success in defining new ufid, increment the counter
          numUfids <- numUfids + 1
          
          # Do a ufid assignment run to catch any stragglers, if this fails
          # it is not such a problem.
          tryCatch(
            ntsportal::es_assign_ufids(escon, udb, index, polarity, new_ufid),
            error = function(cnd) {
              log_error("Error in es_assign_ufids for ufid {new_ufid}")
              log_info("Current mzPosition: {mzPosition}")
              log_info("Current rtPosition: {rtPosition}")
              log_info("Current 1st stage cluster: {cl}")
              log_info("Error text: {conditionMessage(cnd)}")
            }
          )
        }
      }
      # No (more) clusters found after second stage clustering, move to next set of
      # results and continue searching
      log_info("Processed all available candidate clusters, moving to next page
               of features")
      # Get last hit
      mzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]]
      rtPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[2]]
    } else if (length(names(table(dbscanRes$cluster))) == 1 &&
               names(table(dbscanRes$cluster)) == "0") {
      log_info("No 1st stage cluster with 10 or more features found, moving to next page
            of results")
      # Get last hit
      mzPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[1]]
      rtPosition <- res$hits$hits[[length(res$hits$hits)]]$sort[[2]]
    } else {
      stop("Error in 1st stage clustering.")
    }
  }
}
