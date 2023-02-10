

# script adding experimental rt to rtt field of all documents in ntsp

# nohup Rscript ~/projects/ntsportal/tests/add_data/add-rtt.R &> ~/log-files/add-rtt-$(date +%y%m%d).log & 

library(logger)
source("~/connect-ntsp.R")

VERSION <- "2023-01-04"
log_info("-------- add-rtt.R v{VERSION} -------")

# Run script to add  rtt to any docs where this does not exist.

res <- elastic::docs_update_by_query(escon, index = "g2_nts*", body = '
{
  "query": {
    "bool": {
      "filter": [
        {
          "exists": {
            "field": "rt"
          }
        },
        {
          "exists": {
            "field": "chrom_method"
          }
        }
      ],
      "must_not": [
        {
          "nested": {
            "path": "rtt",
            "query": {
              "term": {
                "rtt.predicted": {
                  "value": false
                }
              }
            }
          }
        }
      ]
    }
  },
  "script": {
    "source" : "
      params.entry[0].put(\'rt\', ctx._source[\'rt\']);
      params.entry[0].put(\'method\', ctx._source[\'chrom_method\']);
      if (ctx._source.rtt != null) {
        ctx._source.rtt.add(params.entry);
      } else {
        ctx._source.rtt = params.entry;
      }
    ",
    "params" : {
      "entry" : [
        {
          "predicted" : false
        }
      ]
    }
  } 
}')

log_info("Completed update on {res$updated} docs")
