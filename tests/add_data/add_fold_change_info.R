
# Script to add foldchange information to ogimo data (Moselle longitudinal)


index <- "g2_dbas_ogimo"
source("~/connect-ntsp.R")

all_kms <- elastic::Search(
  escon,
  index,
  body = '
  {
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "river": {
              "value": "mosel"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "kms": {
      "histogram": {
        "field": "km",
        "interval": 5
      }
    }
  }
}
  '
)

akms <- sapply(all_kms$aggregations$kms$buckets, function(x) x$key)
dfkm <- data.frame(km = akms)

# make pos fc table
fc_table <- function(polarity) {
  # 126 is name cardinality pos
  res <- elastic::Search(
    escon, 
    index, 
    body = sprintf(
      '
  {
    "query": {
      "bool": {
        "filter": [
          {
            "term": {
              "river": {
                "value": "mosel"
              }
            }
          },
          {
            "term": {
              "pol": "%s"
            }
          }
        ]
      }
    },
    "size": 0,
    "aggs": {
      "comps": {
        "terms": {
          "field": "name",
          "size": 10000 
        },
        "aggs": {
          "kms": {
            "histogram": {
              "field": "km",
              "interval": 5
            },
            "aggs": {
              "avg_area": {
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
    ', polarity)
  )
  
  dfl <- lapply(res$aggregations$comps$buckets, function(y) {
    data.frame(
      km = sapply(y$kms$buckets, function(x) x$key),
      norm_area = sapply(y$kms$buckets, function(x) if (is.null(x$avg_area$value)) NA else x$avg_area$value)
    )
  })
  
  names(dfl) <- sapply(res$aggregations$comps$buckets, function(x) x$key)
  dfl <- lapply(dfl, function(x) merge(dfkm, x, by = "km", all.x = T))
  dfl <- lapply(dfl, function(x) {x[is.na(x$norm_area), "norm_area"] <- 0; x})
  
  
  shows_fold_change <- function(kmi, df) { # kmi <- 200
    if (kmi == max(df$km) || df[df$km == kmi, "norm_area"] == 0)
      F else df[df$km == kmi, "norm_area"] >= 4*max(df[df$km > kmi, "norm_area"])
  }
  dfl <- lapply(dfl, function(x) {x$fc4 <- sapply(x$km, shows_fold_change, df = x); x})
  
  dfl <- Map(function(x, n) {x$comp <- n; x}, dfl, names(dfl))
  fcResult <- Reduce(rbind, dfl)
  fcResult[fcResult$fc4, ]
}
posres <- fc_table("pos")
posres$pol <- "pos"
negres <- fc_table("neg")
negres$pol <- "neg"
endTab <- rbind(posres, negres)
rownames(endTab) <- NULL
# add this data to the elastic db

for (i in 1:nrow(endTab)) { # i <- 1
  
  elastic::docs_update_by_query(
    escon,
    index,
    body = sprintf(
      '
    {
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "river": {
              "value": "mosel"
            }
          }
        },
        {
          "term": {
            "pol": "%s"
          }
        },
        {
          "term": {
            "name": "%s"
          }
        },
        {
          "range": {
            "km": {
              "gte": %i,
              "lte": %i
            }
          }
        }
      ]
    }
  },
  "max_docs": 1,
  "sort": [
    {
      "norm_a": {
        "order": "desc"
      }
    }
  ],
  "script": {
    "source": "
     if (ctx._source[params.fieldToChange] == null) {
        ctx._source[params.fieldToChange] = [params.newValue];
      } else if (!(ctx._source[params.fieldToChange] instanceof List)) {
        ctx._source[params.fieldToChange] = [ctx._source[params.fieldToChange]];
        ctx._source[params.fieldToChange].add(params.newValue);
      } else {
        ctx._source[params.fieldToChange].add(params.newValue)
      }
    ",
    "params": {
      "fieldToChange": "tag",
      "newValue" : "mosel_prio_foldchange_factor4"
    }, 
    "lang": "painless"
  }
}
    ', endTab[i, "pol"], endTab[i, "comp"], endTab[i, "km"], endTab[i, "km"] + 5)
    
  )
}
