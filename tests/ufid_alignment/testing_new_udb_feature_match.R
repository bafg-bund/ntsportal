
ftt <- tbl(udb, "feature") %>% collect()
rtt <- tbl(udb, "retention_time") %>% collect()
ms2t <- tbl(udb, "ms2") %>% collect()


lapply(
  fts,
  udb_feature_match2,
  ftt = ftt,
  rtt = rtt,
  ms2t = ms2t,
  mztol = config::get("mztol_mda"),
  rttol = config::get("rttol_min"),
  ms2dpThresh = config::get("ms2_ndp_min_score")
)


parallel::mclapply(
  fts,
  udb_feature_match2,
  ftt = ftt,
  rtt = rtt,
  ms2t = ms2t,
  mztol = config::get("mztol_mda"),
  rttol = config::get("rttol_min"),
  ms2dpThresh = config::get("ms2_ndp_min_score"),
  mc.cores = 2,
  mc.preschedule = FALSE
)


system.time(
  lapply(
    fts,
    udb_feature_match,
    udb = udb,
    mztol = config::get("mztol_mda"),
    rttol = config::get("rttol_min"),
    ms2dpThresh = config::get("ms2_ndp_min_score")
  )
)

lapply(
  fts,
  udb_feature_match,
  udb = udb,
  mztol = config::get("mztol_mda"),
  rttol = config::get("rttol_min"),
  ms2dpThresh = config::get("ms2_ndp_min_score")
)

parallel::mclapply(
  fts,
  udb_feature_match,
  udb = udb,
  mztol = config::get("mztol_mda"),
  rttol = config::get("rttol_min"),
  ms2dpThresh = config::get("ms2_ndp_min_score"),
  mc.cores = 2,
  mc.preschedule = FALSE
)
