

source("~/connect-ntsp.R")

res <- elastic::Search(escon, "g2_nts_upb", size = 0, body = '{
  "aggs": {
    "ufids": {
      "terms": {
        "field": "ufid",
        "size" : 2000
      },
      "aggs": {
        "stations": {
          "terms": {
            "field": "station"
          },
          "aggs": {
            "years": {
              "date_histogram": {
                "field": "start",
                "calendar_interval": "year"
              },
              "aggs": {
                "avg_area": {
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
}')

r3 <- lapply(res$aggregations$ufids$buckets, function(x) {
  r2 <- lapply(x$stations$buckets, function(y) {
    r1 <- lapply(y$years$buckets, function(z) {
      if (is.null(z$avg_area$value)) {
        NULL
      } else {
        data.frame(ufid = x$key, station = y$key, start = z$key, area = z$avg_area$value)
      }
    })
    r1 <- Filter(Negate(is.null), r1)
    do.call("rbind", r1)
  })
  do.call("rbind", r2)
})
df <- do.call("rbind", r3)

lmr <- by(df, list(df$ufid, df$station), function(x) {
  if (nrow(x) < 5)
    return(NULL)
  mod <- lm(area ~ start, x)
  data.frame(station = x$station[1], ufid = x$ufid[1], slope = mod$coefficients[2], points = nrow(x))
})
lmr <- Filter(Negate(is.null), lmr)
lmr <- do.call("rbind", lmr)
rownames(lmr) <- NULL
lmr <- lmr[order(lmr$slope, decreasing = T),]
lmr
