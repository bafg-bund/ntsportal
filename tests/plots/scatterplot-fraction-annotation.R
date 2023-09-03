
# create scatterplot to show annotated and non annotated features

library(ggplot2)

source("~/connect-ntsp.R")

# get data for one month Rhine

res <- elastic::Search(escon, "g2_nts_bfg", body = '
                {
  "query": {
    "bool": {
      "must": [
        {
          "term": {
            "station": {
              "value": "rhein_ko_l"
            }
          }
        },
        {
          "term": {
            "pol": {
              "value": "pos"
            }
          }
        },
        {
          "range": {
            "start": {
              "gte": "2021-05-01",
              "lte": "2021-06-01"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "aggs": {
    "ufids": {
      "terms": {
        "field": "ufid",
        "size": 100000
      },
      "aggs": {
        "mz_ufid": {
          "avg": {
            "field": "mz"
          }
        },
        "rt_ufid": {
          "avg": {
            "field": "rt"
          }
        },
        "exists_name": {
          "filter": {
            "exists": {
              "field": "name"
            }
          }
        }
      }
    }
  }
} 
                
                ')

res <- res$aggregations$ufids$buckets

ufids <- data.frame(
  ufid = sapply(res, function(x) x$key),
  mz = sapply(res, function(x) x$mz_ufid$value),
  rt = sapply(res, function(x) x$rt_ufid$value),
  annotated = sapply(res, function(x) x$exists_name$doc_count)
)

ufids$isAnnot <- ifelse(ufids$annotated > 0, TRUE, FALSE)

sum(ufids$isAnnot)

ggplot(ufids, aes(rt, mz, color = isAnnot, alpha = isAnnot)) +
  geom_point(shape = 16) +
  scale_color_manual(values = c("steelblue4", "red")) +
  scale_alpha_manual(values = c(0.3, 1)) +
  theme_bw(14) +
  xlab("Ret. time (min)") + ylab("m/z (Da)") +
  labs(color = "Annotated", alpha = "Annotated") +
  ggtitle("Non-Target Features Detected in Rhine Water at Koblenz",
          subtitle = "May 2021, n = 1945, annotated = 88")
ggsave("tests/plots/230623-scatterplot-rhein-annotated.png", dpi = 300)
