
#' Update the "analysis_dbas" table for Time Series SPM dashboard
#' @description The analysis table shows the slope of the linear regression model for each
#' compound at each station for the DBAS results of yearly composite 
#' samples of SPM (in the `dbas_upb` table). The NTSPortal admin runs this function after
#' processing to update the regression results.
#' @param sourceTableName Table in ntsportal which should be analyzed (only dbas_upb)
#' @export
updateLinearRegressionTable <- function(sourceTableName) {
  dbComm <- getDbComm()
  stopifnot(grepl("feature.*upb", sourceTableName))
  records <- computeLinearRegressionAllEsiSpecs(sourceTableName)
  destTableName <- sub("^(ntsp\\d\\d\\.?\\d)_feature_(.*)$", "\\1_analysis_dbas_\\2", sourceTableName)
  deleteTable(dbComm, destTableName)
  createNewTable(dbComm, destTableName, "analysis_dbas")
  appendRecords(dbComm, destTableName, records)
}

computeLinearRegressionAllEsiSpecs <- function(sourceTableName) {
  allIonSpecs <- getUniqueValues(getDbComm(), sourceTableName, "esi_ion_spec")
  recsList <- map(allIonSpecs, \(x) computeLinearRegressionOneEsiSpec(x, sourceTableName), .progress = TRUE)
  unlist(recsList, recursive = FALSE)
}

computeLinearRegressionOneEsiSpec <- function(esiSpec, sourceTableName) {
  dbComm <- getDbComm()
  resp <- dbComm@client$search(
    index = sourceTableName, 
    query = getQueryForRegressionAnalysis(esiSpec),
    aggs = getAggsForRegressionAnalysis(), 
    size = 0
  )
  
  
  regressionDfList <- list()
  esiSpecSplit <- strsplit(esiSpec, "_")[[1]]
  thisComp <- esiSpecSplit[1]
  thisPol <- esiSpecSplit[2]
  thisAdduct <- esiSpecSplit[3]
  thisIsotop <- esiSpecSplit[4]
  stationBuckets <- resp$body$aggregations$stations$buckets
  if (length(stationBuckets) == 0)
    return(list())
  for (station in stationBuckets) {
    thisStation <- station$key
    time_buckets <- station$time_course$buckets
    if (length(time_buckets) < 3)
      next
    # calculate regression and add this row to list
    df <- data.frame(
      norma = vapply(time_buckets, function(x) { 
        if (is.null(x$average_area_relative_to_internal_standard$value)) {
          NA
        } else {
          x$average_area_relative_to_internal_standard$value
        }
      }, numeric(1)),
      time = sapply(time_buckets, function(x) lubridate::ymd_hms(x$key_as_string, tz = "UTC"))
    )
    # Need to normalize the regression to the max area 
    
    df$norma <- df$norma / max(df$norma, na.rm = T)
    model <- lm(norma ~ time, df)
    slope <- model$coefficients["time"]
    newRow <- data.frame(
      name = thisComp,
      pol = thisPol,
      adduct = thisAdduct,
      isotopologue = thisIsotop,
      station = thisStation,
      change_per_ms = slope,
      datapoints = length(time_buckets)
    )
    regressionDfList[[length(regressionDfList) + 1]] <- newRow
    
  }
  if (length(regressionDfList) == 0)
    return(list())
  regDf <- do.call("rbind", regressionDfList)
  rownames(regDf) <- NULL
  regDf$lm <- regDf$change_per_ms * 1000 * 60 * 60 * 24 * 365
  
  # make data ready for elastic
  regDf$change_per_ms <- NULL
  regDf$datapoints <- NULL
  
  regDf$matrix <- "spm"
  regList <- split(regDf, 1:nrow(regDf))
  names(regList) <- NULL
  lapply(regList, newNtspRecord)
}

getQueryForRegressionAnalysis <- function(esiIonSpec) {
  list(bool = list(must = list(
    list(term = list(esi_ion_spec = esiIonSpec)),
    list(term = list(duration = "P1Y")),
    list(exists = list(field = "area_internal_standard")),
    list(exists = list(field = "area")),
    list(nested = list(
      path = "compound_annotation", query = list(exists = list(field = "compound_annotation.name"))
    ))
  )))
}

getAggsForRegressionAnalysis <- function() {
  list(
    stations = list(
      terms = list(
        field = "station",
        size = 1000
      ),
      aggs = list(
        time_course = list(
          date_histogram = list(
            field = "start",
            calendar_interval = "year"
          ),
          aggs = list(
            average_area_relative_to_internal_standard = list(
              avg = list(
                field = "area_relative_to_internal_standard"
              )
            ),
            average_intensity_relative_to_internal_standard = list(
              avg = list(
                field = "intensity_relative_to_internal_standard"
              )
            ),
            average_area = list(
              avg = list(
                field = "area"
              )
            ),
            average_intensity = list(
              avg = list(
                field = "intensity"
              )
            )
          )
        )
      )
    )
  )
}


# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
