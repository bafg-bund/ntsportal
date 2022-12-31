# script to add formula to any index

# create connections
library(dplyr)
index <- "g2_dbas_v6_sachsen"

sdb <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v9.db")
ctb <- tbl(sdb, "compound") %>% select(name, formula) %>% collect()
DBI::dbDisconnect(sdb)
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https")

# get all compound names

res <- elastic::Search(escon, index, body = '
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
      }
    }
  }
}
')

# write table for checking
allComps <- vapply(res$aggregations$comps$buckets, "[[", character(1), i = "key")

#setdiff(allComps, ctb$name)
# for each name, get formula 

fdf <- data.frame(name = allComps, formula = sapply(allComps, function(x) ctb[ctb$name == x, "formula", drop = T]))
rownames(fdf) <- NULL
for (i in seq_len(nrow(fdf))) {
  message("processing ", fdf[i, "name"])
  elastic::docs_update_by_query(escon, index, body = sprintf('
  {
    "query": {
      "term": {
        "name": {
          "value": "%s"
        }
      }
    },
    "script": {
      "source": "ctx._source.formula = params.form",
      "params": {
        "form": "%s"
      }, 
      "lang": "painless"
    }
  }
  ', fdf[i, "name"], fdf[i, "formula"]))
  message("complete")
}


