
# Script to add comp_group information to index.

# if you are updating an index which has already been processed by this script, you must first
# remove the previous comp_group information. e.g.:

# POST g2_dbas_v4_bfg/_update_by_query?wait_for_completion=false
# {
#   "query": {
#    "exists": {
#      "field": "comp_group"
#    }
#  },
#   "script": {
#     "source": "ctx._source.remove('comp_group')",
#     "lang": "painless"
#   }
# }


library(dplyr)
index <- "g2_dbas_v5_bfg"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https")

db <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v9.db")



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



