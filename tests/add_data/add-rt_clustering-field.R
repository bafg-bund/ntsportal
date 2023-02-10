
# script to add rt_clustering field to all docs

# nohup Rscript ~/projects/ntsportal/tests/add_data/add-rt_clustering-field.R &> ~/log-files/add-rt_clustering-$(date +%y%m%d).log &


library(logger)
source("~/connect-ntsp.R")
index <- "g2_nts*"

VERSION <- "2023-02-01"
log_info("-------- add-rt_clustering.R v{VERSION} -------")

# get all features without rt_clustering
repeat {
  res <- elastic::Search(escon, index, body = '
                       {
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "rt_clustering"
          }
        }
      ]
    }
  },
  "_source": ["rtt"],
  "size": 10000
}
                       
                       ')
  if (res$hits$total$value == 0)
    break
  
  log_info("There are {res$hits$total$value} left to process")  
  
  fts <- res$hits$hits
  for (ft in fts) { #ft <- fts[[1]]
    # get rt
    doc <- ft[["_source"]]
    esid <- ft[["_id"]]
    esind <- ft[["_index"]]
    rtToCopy <- doc$rtt[[which(sapply(doc$rtt, function(x) x$method == "bfg_nts_rp1"))]]$rt
    elastic::docs_update(escon, esind, esid, body = sprintf('
{
  "script" : {
    "source": "ctx._source.rt_clustering = params.rtToCopy;",
    "params": {
      "rtToCopy": %.2f
    }
  }
}
', rtToCopy))
  }

}

log_info("Completed all features")
