
index <- "g2_dbas_v5_upb"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

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
