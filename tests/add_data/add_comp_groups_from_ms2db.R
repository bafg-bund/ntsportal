
library(dplyr)
index <- "g2_dbas_v5_upb"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

db <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v7.db")



# addition of compound groups ####
# get list of all compounds in db

res <- elastic::Search(escon, index, body = 
'
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
'
                         )
comps <- sapply(res$aggregations$comps$buckets, function(x) x$key)

# function to get compound groups for each compound
#DBI::dbListTables(db)
#DBI::dbListFields(db, "compoundGroup")
get_groups <- function(compName) {  #compName <- comps[1]
  compt <- tbl(db, "compound")
  cg <- tbl(db, "compoundGroup")
  cgc <- tbl(db, "compGroupComp")
  groups <- filter(compt, name == compName) %>% select(compound_id) %>% 
    left_join(cgc) %>% select(compoundGroup_id) %>% left_join(cg) %>% 
    select(name) %>% collect() %>% unlist()
  groups <- groups[groups != "BfG"]
  groups
}

compGroups <- lapply(comps, get_groups)
names(compGroups) <- comps
compGroups <- Filter(function(x) length(x) != 0, compGroups)

# loop through each compound and add classifications to elastic documents
#any(duplicated(names(compGroups)))

for (cp in names(compGroups)) {
  for (gr in compGroups[[cp]]) {
    elastic::docs_update_by_query(escon, index, refresh = "true", body = sprintf(
      '
      {
        "query": {
          "term": {
            "name": {
              "value": "%s"
            }
          }
        },
        "script": {
          "source": "
            if (ctx._source.comp_group == null) {
              ctx._source.comp_group = params.newGroup;
            } else {
              ctx._source.comp_group = [ctx._source.comp_group];
              ctx._source.comp_group.add(params.newGroup);
            }
          ",
          "lang": "painless",
          "params": {
            "newGroup": "%s"
          }
        }
      }
      ',cp, gr)
    )
    message("completed ", gr, " of comp ", cp)
  }
}


DBI::dbDisconnect(db)



