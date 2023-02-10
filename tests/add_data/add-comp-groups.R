
# Script to add comp_group information to index.

# if you are updating an index which has already been processed by this script, you must first
# remove the previous comp_group information. e.g.:

# nohup Rscript add-comp-groups.R g2_dbas_upb &> ~/log-files/add-comp-groups-$(date +%y%m%d).log &


library(dplyr)
library(logger)
source("~/connect-ntsp.R")

VERSION <- "2023-01-13"

# allowed comp groups, as they are currently formated in sdb
# ntsp uses all lower case
COMPGROUPS <- c(
"Pharmaceutical",  
"Transformation_product", 
"Antimicrobial",     
"Food_additive",       
"Fungicide",     
"Herbicide",         
"Industrial_process", 
"Insecticide",         
"Metabolite",         
"Natural_product",  
"Personal_care_product",
"Pesticide",               
"Pigment"               
) 

index <- commandArgs(TRUE)
#index <- "g2_nts_bfg"

log_info("----- add-comp-groups.R v{VERSION} -----")
log_info("Update on index {index}")
log_info("{length(COMPGROUPS)} allowed groups: {paste(COMPGROUPS, collapse = ', ')}")


if (!is.character(index) || length(index) != 1 || !grepl("^g2_[dbasnts]{3,4}_", index))
  stop("Incorrect index specification")
# Will hang if index is not present
log_info("{elastic::count(escon, index)} docs in index")

db <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v9.db")


# check that allowed comp groups are all in database
testGroups <- tbl(db, "compoundGroup") %>% collect()
if(!all(COMPGROUPS %in% testGroups$name))
  stop("not all allowed comp groups are found in db")

# first delete current compound groups, making sure there is no duplication

log_info("Removing current comp_group field")


res <- elastic::docs_update_by_query(escon, index, body = '
{
  "query": {
   "exists": {
     "field": "comp_group"
   }
 },
  "script": {
    "source": "ctx._source.remove(\'comp_group\')",
    "lang": "painless"
  }
}
')


if (res$timed_out)
  stop("Unsuccessful removal of comp_group field")

log_info("Removed comp_group from {res$total} docs, adding new comp_group")
rm(res)
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
        "size": 10000
      }
    }
  }
}
')
comps <- sapply(res$aggregations$comps$buckets, function(x) x$key)

# function to get compound groups for each compound
#DBI::dbListTables(db)
#DBI::dbListFields(db, "compoundGroup")
get_groups <- function(compName) {  #compName <- comps[1]
  compt <- tbl(db, "compound")
  cg <- tbl(db, "compoundGroup")
  cgc <- tbl(db, "compGroupComp")
  groups <- filter(compt, name == compName) %>% select(compound_id) %>% 
    left_join(cgc, by = "compound_id") %>% select(compoundGroup_id) %>% 
    left_join(cg, by = "compoundGroup_id") %>% 
    select(name) %>% collect() %>% unlist() %>% unname()
  groups <- groups[groups %in% COMPGROUPS]
  groups <- tolower(groups)
  groups
}

compGroups <- lapply(comps, get_groups)
names(compGroups) <- comps
compGroups <- Filter(function(x) length(x) != 0, compGroups)

log_info("Updating comp_group on {length(compGroups)} compounds")

# loop through each compound and add classifications to elastic documents

for (cp in names(compGroups)) {
  for (gr in compGroups[[cp]]) {
    #log_info("Updating {cp} with {gr}")
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
  }
}


DBI::dbDisconnect(db)

log_info("----- Completed add-comp-groups.R on index {index} -----")


