library(elastic)

index <- "g2_nts1_bfg"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)
escon$ping()

response <- elastic::Search(escon, index, body = '
                            {
  "query": {
    "range": {
      "start": {
        "gte": "2021-04-01",
        "lte": "2021-04-30"
      }
    }
  },
  "aggs": {
    "ufid": {
      "terms": {
        "field": "ufid",
        "size": 10000
      },
      "aggs": {
        "verlauf": {
          "date_histogram": {
            "field": "start",
            "calendar_interval": "day"
          },
          "aggs": {
            "avg_area": {
              "avg": {
                "field": "area_normalized"
              }
            },
            "annotations": {
              "terms": {
                "field": "name",
                "size": 10
              }
            }
          }
        }
      }
    }
  },
  "size": 0
}
                            ')

ufids <- response$aggregations$ufid$buckets

tables <- lapply(ufids, function(x) {
  ver <- x$verlauf$buckets
  rows <- lapply(ver, function(y) {
    if (is.numeric(y$avg_area$value)) {
      if (length(y$annotations$buckets) > 0) {
        data.frame(
          ufid = x$key, date = y$key_as_string,
          area_normalized = y$avg_area$value,
          name = paste(sapply(y$annotations$buckets, function(z) {z$key}), collapse = ", ")
        )
      } else {
        data.frame(ufid = x$key, date = y$key_as_string, area_normalized = y$avg_area$value, name = "unknown")
      }
    } else {
      data.frame(ufid = x$key, date = y$key_as_string, area_normalized = 0, name = NA)
    }
  })
  rows <- Filter(Negate(is.null), rows)
  do.call("rbind", rows)
})
names(tables) <- paste("ufid", sapply(ufids, "[[", "key"), sep = "_")
View(tables[[1]])
View(do.call("rbind", tables))
