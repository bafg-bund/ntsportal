
index <- "g2_dbas_upb"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic.dmz.bafg.de', port = 443, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https", ssl_verifypeer = FALSE)

res <- elastic::Search(
  escon, index, 
  body = '
{
  "query": {
    "exists": {
      "field": "comp_group"
    }
  },
  "size": 0,
  "aggs": {
    "groups": {
      "terms": {
        "field": "comp_group",
        "size": 20
      },
      "aggs": {
        "compounds": {
          "cardinality": {
            "field": "name"
          }
        }
      }
    }
  }
}
  ')

df <- data.frame(group = sapply(res$aggregations$groups$buckets, "[[", i = "key"),
           num_compounds = sapply(res$aggregations$groups$buckets, function(x) x$compounds$value)
           )


res2 <- elastic::Search(
  escon, 
  index,
  body =
    '
  {
  "query": {
    "match_all": {}
  },
  "size": 0,
  "aggs": {
    "stations": {
      "terms": {
        "field": "station",
        "size": 50
      },
      "aggs": {
        "years": {
          "date_histogram": {
            "field": "start",
            "calendar_interval": "year"
          }
        }
      }
    }
  }
}
  '
)
stations <- res2$aggregations$stations$buckets

sy <- lapply(stations, function(x) {
  years <- x$years$buckets
  sapply(years, "[[", i = "key_as_string")
})
names(sy) <- sapply(stations, "[[", i = "key")
sy
sy <- lapply(sy, function(x) substr(x, 1, 10))
sy <- lapply(sy, function(x) lubridate::ymd(x))
allY <- unique(Reduce(c, sy))
sy <- Map(function(sta, y) data.frame(station = sta, years = y), names(sy), sy)
sy <- Reduce(rbind, sy)
library(ggplot2)
nums <- by(sy, sy$station, nrow)
stalim <- names(nums[order(nums, decreasing = F)])
stalab <- read.csv("tests/summary_scripts/station-codes.csv")$city
names(stalab) <- read.csv("tests/summary_scripts/station-codes.csv")$code

ggplot(sy, aes(x = years, y = station)) + geom_count(show.legend = F) +
  scale_y_discrete(limits = stalim, labels = stalab[stalim]) +
  scale_x_date(breaks = allY, date_labels = "%y") +
  theme_grey(20) +
  xlab("Jahr (20-)") + ylab("Messstelle") +
  ggtitle("Eingetragene Jahre in NTSPortal", subtitle = "Schwebstoffproben UPB (Screening), Stand: 2022-11-08")
ggsave("~/HRMS_Z/NTS_Portal/Kevin/eingetragene-jahre-spm.png")       
