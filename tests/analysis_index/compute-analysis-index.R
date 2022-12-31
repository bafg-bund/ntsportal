# script to create a new analysis index, which can be used to prioritize 
# compounds by their trend. E.g. Regression trend

# We do this first for the _upb database

# collect data by compound and station

INDEX <- "g2_dbas_upb"
ANALYSIS_INDEX <- "g2_dbas_upb_analysis"
dateId <- format(Sys.Date(), "%y%m%d")
SAVE_LOC <- "~/projects/ntsportal/tests/analysis_index"
INGEST_SCRIPT <- "~/projects/ntsautoeval/ingest.sh"
CONFIG_PATH <- "~/config.yml"
logFile <- file.path(SAVE_LOC, paste0(dateId, ".log")) 
cat("----- compute-analysis-index.R v2022-10-28 -----\n", file = logFile)
cat(date(), "\n", file = logFile, append = T)

ec <- config::get("elastic_connect", file = CONFIG_PATH)
escon <- elastic::connect(host = 'elastic.dmz.bafg.de', port = 443, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https", ssl_verifypeer = FALSE)

if (escon$ping()$cluster_name == "bfg-elastic-cluster")
  cat("Connection to elasticsearch successful\n", file = logFile, append = TRUE)

#escon$ping()
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
                          "field": "norm_a"
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
  cat("Deleting old json\n", file = logFile, append = TRUE)
  file.remove(file.path(SAVE_LOC, "reg.json"))
}

cat("Writing new json\n", file = logFile, append = TRUE)
jsonlite::write_json(reg, file.path(SAVE_LOC, "reg.json"), pretty = T, digits = NA, auto_unbox = T)

# clear contents of current analysis index
cat("Clearing old data\n", file = logFile, append = TRUE)
mes <- elastic::docs_delete_by_query(escon, ANALYSIS_INDEX, '{
  "query": {
    "match_all": {}
  }
}')
cat(mes$deleted, "docs cleared\n", file = logFile, append = TRUE)

# upload newly formed index
cat("Uploading new docs\n", file = logFile, append = TRUE)

if (file.exists(file.path(SAVE_LOC, "reg.json")))
  ingestMes <- system(paste(INGEST_SCRIPT, CONFIG_PATH, ANALYSIS_INDEX, file.path(SAVE_LOC, "reg.json")), intern = T)

cat(ingestMes, "\n", file = logFile, append = TRUE, fill = TRUE)






