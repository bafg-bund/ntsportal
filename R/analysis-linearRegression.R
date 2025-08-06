
#' Update the "analysis_dbas" table for Time Series SPM dashboard
#' @description The analysis table shows the slope of the linear regression model for each
#' compound at each station for the DBAS results of yearly composite 
#' samples of SPM (in the `dbas_upb` table). The NTSPortal admin runs this function after
#' processing to update the regression results.
#' @param sourceTableName Table in ntsportal which should be analyzed (only dbas_upb)
#' @export
updateLinearRegressionTable <- function(sourceTableName) {
  dbComm <- getDbComm()
  stopifnot(grepl("dbas.*upb", sourceTableName))
  queryBlock = list(term = list(duration = 365))
  destTableName <- sub("^(ntsp\\d\\d\\.?\\d)_dbas_(.*)$", "\\1_analysis_dbas_\\2", sourceTableName)
  records <- computeLinearRegression(sourceTableName, queryBlock)
  deleteTable(dbComm, destTableName)
  createNewTable(dbComm, destTableName, "analysis_dbas")
  appendRecords(dbComm, destTableName, records)
}

computeLinearRegression <- function(sourceTableName, queryBlock = list()) {
  dbComm <- getDbComm()
  if (length(queryBlock) == 0) 
    queryBlock <- list(match_all = stats::setNames(list(), character(0)))
  
  resp <- dbComm@client$search(
    index = sourceTableName, 
    query = queryBlock,
    aggs = getAggsForRegressionAnalysis(), 
    size = 0
  )
  
  res <- resp$body
  regressionDfList <- list()
  
  for (comp in res$aggregations$comps$buckets) {
    comp_name <- comp$key
    for (pol in comp$pols$buckets) {
      pol_name <- pol$key
      if (length(pol$stations$buckets) != 0) {
        for (station in pol$stations$buckets) {
          station_name <- station$key
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
          newRow <- data.frame(name = comp_name, pol = pol_name, station = station_name, change_per_ms = slope, datapoints = length(time_buckets))
          regressionDfList[[length(regressionDfList) + 1]] <- newRow
          
        }
      }
    }
  }
  
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

getAggsForRegressionAnalysis <- function() {
  list(
    comps = list(
      terms = list(
        field = "name", 
        size = 1000
      ),
      aggs = list(
        pols = list(
          terms = list(
            field = "pol",
            size = 2
          ),
          aggs = list(
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
        )
      )
    )
  )
}


# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
