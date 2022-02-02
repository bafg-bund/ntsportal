# Addition of inchi-key annotation to database.
library(dplyr)
index <- "g2_dbas_v5_upb"

sdb <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v7.db")
ctb <- tbl(sdb, "compound") %>% select(name, SMILES) %>% collect()
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)


# get all names from index

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
        "size": 600
      }
    }
  }
}
                       ')

# write table for checking
allComps <- vapply(res$aggregations$comps$buckets, "[[", character(1), i = "key")

# for each name, get SMILES from Spektrendatenbank
# for smiles, convert to inchikey with obabel
allInKey <- vapply(allComps, function(x) {
  message("converting ", x)
  smiles <- ctb[ctb$name == x, "SMILES", drop = TRUE]
  stopifnot(is.character(smiles), length(smiles) == 1)
  inchikey <- system(sprintf("echo '%s' | obabel -ismi -oinchikey", smiles), intern = TRUE)
  message("done")
  inchikey
}, character(1))

df <- data.frame(name = allComps, inchikey = allInKey)

# update es with inchikey
for (i in seq_len(nrow(df))) {
  message("processing ", df[i, "name"])
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
      "source": "ctx._source.inchikey = params.inkey",
      "params": {
        "inkey": "%s"
      }, 
      "lang": "painless"
    }
  }
  ', df[i, "name"], df[i, "inchikey"]))
  message("complete")
}

DBI::dbDisconnect(sdb)
 