library(dplyr)
index <- "g2_dbas_v4_ogimo"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

db <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v7.db")

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
              "value": "Mosel"
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
fc_table <- function(polarity, cardinality) {
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
                "value": "Mosel"
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
          "size": %i  
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
    ', polarity, cardinality)
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
posres <- fc_table("pos", 126L)
posres$pol <- "pos"
negres <- fc_table("neg", 29L)
negres$pol <- "neg"
endTab <- rbind(posres, negres)
rownames(endTab) <- NULL
# add this data to the elastic db

for (i in seq_len(nrow(endTab))) { # i <- 50
  
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
              "value": "Mosel"
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
    "source": "ctx._source.tag = params.entry",
    "params": {
      "entry" : "mosel_prio_foldchange_factor4"
    }, 
    "lang": "painless"
  }
}
    ', endTab[i, "pol"], endTab[i, "comp"], endTab[i, "km"], endTab[i, "km"] + 5)
    
  )
}
