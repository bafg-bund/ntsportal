

library(ntsportal)
source("~/connect-ntsp.R")
res <- es_search_paged(escon, "g2_dbas*", searchBody = '{
  "query": {
    "term": {
      "station": {
        "value": "mosel_ko_r"
      }
    }
  },
  "_source": ["name", "inchikey", "pol", "start", "duration", "area", "intensity", "area_is", "area_normalized"]
}', sort = "mz")

temp <- lapply(res$hits$hits, function(x) as.data.frame(x[["_source"]]))
df <- plyr::rbind.fill(temp)



