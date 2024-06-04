# script to create a new analysis index, which can be used to prioritize 
# compounds by their trend. E.g. Regression trend

# We do this first for the _upb database

# collect data by compound and station

INDEX <- "ntsp_dbas_upb"
ANALYSIS_INDEX <- "ntsp_dbas_analysis_upb"
SAVE_LOC <- "/scratch/nts/tmp"
INGEST_SCRIPT <- "scripts/ingest.sh"
CONFIG_PATH <- "~/config.yml"
source("~/connect-ntsp.R")
library(logger)

log_info("----- compute-analysis-index.R v2024-04-18 -----")

res <- elastic::Search(
  escon, INDEX, 
  body = '
  {
    "query": {
      "match_all": {}
    },
    "size": 0,
    "aggs": {
      "comps": {
        "terms": {
          "field": "name",
          "size": 1000
        },
        "aggs": {
          "pols": {
            "terms": {
              "field": "pol",
              "size": 2
            },
            "aggs": {
              "stations": {
                "terms": {
                  "field": "station",
                  "size": 100
                },
                "aggs": {
                  "time_course": {
                    "date_histogram": {
                      "field": "start",
                      "calendar_interval": "year"
                    },
                    "aggs": {
                      "average_norm_a": {
                        "avg": {
                          "field": "area_normalized"
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  ')


regList <- list()
# comp <- res$aggregations$comps$buckets[[1]]
# pol <- comp$pols$buckets[[1]]
# station <- pol$stations$buckets[[1]]

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
          norma = vapply(time_buckets, function(x) if (is.null(x$average_norm_a$value)) NA else x$average_norm_a$value, numeric(1)),
          time = vapply(time_buckets, function(x) x$key, numeric(1))
        )
        #df$time <- df$time / 1000  # time in secs since epoch 
        # need to normalize the regression to the max area otherwise you are just going to see the
        # highest intensity peaks
        
        df$norma <- df$norma / max(df$norma, na.rm = T)
        model <- lm(norma ~ time, df)
        slope <- model$coefficients["time"]
        newRow <- data.frame(name = comp_name, pol = pol_name, station = station_name, change_per_ms = slope, datapoints = length(time_buckets))
        regList[[length(regList) + 1]] <- newRow
        
      }
    }
  }
}

reg <- do.call("rbind", regList)
rownames(reg) <- NULL
reg$lm <- reg$change_per_ms * 1000 * 60 * 60 * 24 * 365

# make data ready for elastic
reg$change_per_ms <- NULL
reg$datapoints <- NULL

if (file.exists(file.path(SAVE_LOC, "reg.json"))) {
  log_info("Deleting old json")
  file.remove(file.path(SAVE_LOC, "reg.json"))
}

reg$matrix <- "spm"

log_info("Writing new json")
jsonlite::write_json(reg, file.path(SAVE_LOC, "reg.json"), pretty = T, digits = NA, auto_unbox = T)

# clear contents of current analysis index
log_info("Clearing old data")
mes <- elastic::docs_delete_by_query(escon, ANALYSIS_INDEX, '{
  "query": {
    "match_all": {}
  }
}')
log_info(mes$deleted, "docs cleared")

# upload newly formed index
log_info("Uploading new docs")

if (file.exists(file.path(SAVE_LOC, "reg.json")))
  ingestMes <- system(paste(INGEST_SCRIPT, CONFIG_PATH, ANALYSIS_INDEX, file.path(SAVE_LOC, "reg.json")), intern = T)

log_info("Completed compute-analysis-index.R")


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
# with ntsportal If not, see <https://www.gnu.org/licenses/>.



