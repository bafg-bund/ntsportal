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


# Internal functions ####



#' @export
norm_ms2 <- function(x, precursorMz, noiselevel = 0.01) {
  # remove noise only if there are a lot of peaks
  if (nrow(x) > 10) {
    maxInt <- max(x[x$mz < precursorMz -1, "int"])
    x <- x[x$int >= noiselevel * maxInt, ]
  }
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / max(x$int))
}

#' Aggregate MS2 Spectra 
#' 
#' @description Takes a series of MS2 spectra and combines these into one averaged spectrium based on binning fragment intensities.
#' 
#' @param x `data.frame` containing the m/z and intensity of all fragments to be combined (spectra are row binded together)
#' 
#' @details Goes through the a data.frame of fragments starting with the most intense fragments and combines
#' fragments which are within the mztolerance into one centroid with the average m/z. Potential
#' problem with this is that instruments with radically different response factors and noise levels
#' can not be aggregated together in this way. A more generalized approach is needed in the future
#' based on the density distribution of spectra.
#' 
#' @returns Spectrum as a `data.frame` with the same column names as the input table
#' @export
clean_spectrum <- function(x, mztol = 0.005, msLevel = "ms1", precursorMz = NULL) {
  stopifnot(is.data.frame(x), ncol(x) == 2, colnames(x)[1] == "mz")
  oldColNames <- colnames(x)
  colnames(x) <- c("mz", "int")
  work <- x[order(x$int, decreasing = TRUE), ]
  new_spec <- list()
  work <- as.matrix(work)
  while (nrow(work) > 0) {
    mz_ <- work[1, "mz", drop = T]
    rows_ <- abs(work[, "mz"] - mz_) <= mztol
    new_row <- c(mean(work[rows_, "mz", drop = T]), sum(work[rows_, "int", drop = T]))
    new_spec[[length(new_spec)+1]] <- new_row
    work <- work[!rows_, , drop = F]
  }
  newSpec <- as.data.frame(do.call("rbind", new_spec))
  colnames(newSpec) <- c("mz", "int")

  if (!is.null(precursorMz) && msLevel == "ms1") {
    newSpec <- norm_ms1(newSpec, precursorMz, noiselevel = 0.01)
  } else if (msLevel == "ms2") {
    newSpec <- norm_ms2(newSpec, precursorMz, noiselevel = 0.02)
  } else {
    stop("error in clean_spectrum")
  }
  colnames(newSpec) <- oldColNames
  newSpec
}

# TODO This may need to be combined with eval_dbas
#' @export
calc_ndp_fit <- function(d_spec, db_spec, ndp_m = 2, ndp_n = 1, mztolu_ = 0.015) {
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

  # d_int <- append(d_int, d_nonmatching[, 2])
  # masses <- append(masses, d_nonmatching[, 1])
  # db_int <- append(db_int, rep(0, nrow(d_nonmatching)))

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

# Alignment functions ####


#' Find ufid for feature
#'
#' @description Recieves a feature and returns its ufid_assignment object based on
#' information in the ufid database. Currently, the feature must have m/z,
#' rt and ms2. Tables are precollected from the SQLite ufid library as tibbles 
#' to increase speed and allow parallelizatation
#'
#' @param ftt Precollected feature table from ufid-db
#' @param rtt Precollected retention_time table from ufid-db
#' @param ms2t Precollected ms2 table from ufid-db
#' @param ft `feature` object
#' @param mztol m/z tolerance in Da
#' @param rttol Retention time tolerance
#' @param ms2dpThresh Threshold for dot-product comparison
#'
#' @returns ntsportal::ufid_assignment object (ufid and level) Currently only level 1 is
#' possible. Returns ufid_assignment of 0, level 0 if no match is found.
#'
#' @export
udb_feature_match <- function(ftt, rtt, ms2t, ft, mztol = 0.007, rttol = 1, ms2dpThresh = 50) {
  ft <- validate_feature(ft)

  # check for mz in db

  detmz <- ftt %>%
    filter(polarity == !!ft$pol) %>%
    filter(between(mz, !!ft$mz - mztol, !!ft$mz + mztol)) %>%
    select(ufid, mz)

  # mass not found, end
  if (nrow(detmz) == 0)
    return(new_ufid_assignment(0L, 0L))

  # check for rt with appropriate method

  # compare only based on bfg method (experimental or predicted)
  # later add differentiation experimental/predicted
  #rt_ <- subset(ft$rtt, method == "bfg_nts_rp1", rt, drop = TRUE)
  rt_ <- ft$rt

  detrt <- rtt %>%
    filter(ufid %in% !!detmz$ufid) %>%
    filter(method == "bfg_nts_rp1") %>%  # use the same method name across DBs!
    filter(between(rt, rt_ - rttol, rt_ + rttol)) %>%
    select(ufid)


  # check ms2



  stopifnot("ms2" %in% names(ft))

  ms2s <- ms2t %>%
    filter(ufid %in% !!detmz$ufid) %>%
    filter(method == "bfg_nts_rp1") %>%
    select(mz, rel_int, ufid)

  if (nrow(ms2s) > 0) {
    ms2l <- split(ms2s, ms2s$ufid)
    ms2_ <- ft$ms2
    # compare ms2 spectra
    dpms2 <- function(dbspec) {
      #browser()
      z <- calc_ndp_fit(
        ft$ms2,
        as.data.frame(dbspec[, c("mz", "rel_int")]),
        ndp_m = config::get("ms2_ndp_m"),
        ndp_n = config::get("ms2_ndp_n"),
        mztolu_ = config::get("mztol_ms2_ndp_mda") / 1000
      )
      data.frame(score = round(z), ufid = dbspec$ufid[1])
    }

    ms2scoresl <- lapply(ms2l, dpms2)
    ms2scores <- do.call("rbind", ms2scoresl)
    ms2scores$score <- ifelse(ms2scores$score >= ms2dpThresh, 1, 0)
  }

  # return ufid assignment with level
  # combine ufids
  # at the moment we need ms2 scores, otherwise it won't work
  stopifnot(exists("ms2scores"))


  ufid_list <- list(mz = detmz$ufid, rt = detrt$ufid)
  if (exists("ms2scores") && nrow(ms2scores) > 0)
    ufid_list[["ms2"]] <- ms2scores$ufid

  # ufid_list must contain mz, rt and ms2 at this stage
  if (!all(is.element(c("mz", "rt", "ms2"), names(ufid_list)))) {
    message("ufid_list does not contain all 3 elements")
    return(new_ufid_assignment(0L, 0L))
  }

  inter_ufid <- Reduce(union, ufid_list)

  # get combined score for each ufid
  extract_values <- function(u_num, msXscores) {
    temp <- subset(msXscores, ufid == u_num, score, drop = T)
    ifelse(length(temp) == 0, 0, temp)
  }

  ufid_scores <- data.frame(
    ufid = inter_ufid,
    mz_score = vapply(inter_ufid, function(y) ifelse(y %in% detmz$ufid, 1, 0), numeric(1)),
    rt_score = vapply(inter_ufid, function(y) ifelse(y %in% detrt$ufid, 1, 0), numeric(1)),
    ms2_score = vapply(inter_ufid, extract_values, numeric(1), msXscores = ms2scores)
  )

  # At the time this assignment is just for level 1 case
  ufid_scores$sum <- apply(ufid_scores[, 2:4], 1, sum)
  ufid_scores <- subset(ufid_scores, sum == 3)
  if (nrow(ufid_scores) == 0)
    return(new_ufid_assignment(0L, 0L))
  winner <- ufid_scores$ufid[which.max(ufid_scores$sum)]
  new_ufid_assignment(as.integer(winner), 1L)
}



#' Find if feature and specified ufid entry are a match
#'
#' @param ft `feature` object
#' @param ftt precollected feature table from ufid-db
#' @param rtt precollected retention time table from ufid-db
#' @param ms2t precollected ms2 table from ufid-db
#' @param ufid_to_match Universal feature ID which is to be tested
#' @param mztol m/z tolerance
#' @param rttol retention time tolerance
#' @param ms2dpThresh MS2 comparison threshold
#' @param ndp_m Normalized dot product variable m
#' @param ndp_n Normalized dot product variable n
#' @param mztolms2 MS2 m/z tolerance
#' @param chromMethod Chromatographic method to extract retention time
#'
#' @return TRUE if matching
#' @export
udb_feature_to_ufid_match <- function(
    ft, ftt, rtt, ms2t, ufid_to_match, mztol = 0.007, rttol = 1, ms2dpThresh = 50, 
    ndp_m = 1, ndp_n = 1, mztolms2 = 0.03, chromMethod = "bfg_nts_rp1") {
  ft <- validate_feature(ft)
  # get mz
  dbmz <- ftt %>%
    filter(ufid == !!ufid_to_match) %>%
    select(mz) %>% .$mz
  stopifnot(length(dbmz) == 1)

  # get rt

  dbrt <- rtt %>% filter(ufid == !!ufid_to_match) %>%
    select(rt) %>% .$rt
  stopifnot(length(dbrt) == 1)
  
  dataRt <- subset(ft$rtt, method == chromMethod, "rt", drop = T)
  stopifnot(length(dataRt) == 1)
  # get ms2

  dbms2 <- ms2t %>% filter(ufid == !!ufid_to_match) %>% select(mz, rel_int)
  # ms2 in ntsp is not relative to max, whereas data in ufid-db is. 
  # The noiselevel should be irrelevant, since this was cleared before ingest.
  dataMs2 <- ntsportal::norm_ms2(ft$ms2, ft$mz, noiselevel = 0.0001)
  ms2similarity <- ntsportal::calc_ndp_fit(
    dataMs2,
    as.data.frame(dbms2),
    ndp_m = ndp_m,
    ndp_n = ndp_n,
    mztolu_ = mztolms2
  )
  all(abs(dbmz - ft$mz) <= mztol, abs(dbrt - dataRt) <= rttol, ms2similarity >= ms2dpThresh)
}

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
# with ntsworkflow. If not, see <https://www.gnu.org/licenses/>.
