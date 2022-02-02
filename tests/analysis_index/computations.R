# script to create a new analysis index, which can be used to prioritize 
# compounds by there trend. E.g. Regression trend

# We do this first for the _upb database

# collect data by compound and station

index <- "g2_dbas_v5_upb"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)


res <- elastic::Search(
  escon, index, 
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
reg$lm <- reg$change_per_ms * 1000 * 60 * 60 * 24

# make data ready for elastic
reg$change_per_ms <- NULL
reg$datapoints <- NULL

jsonlite::write_json(reg, "reg.json", pretty = T, digits = NA, auto_unbox = T)
