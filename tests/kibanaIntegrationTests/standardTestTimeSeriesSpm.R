

# Test display of spm time series (standard test, no changes)

# if you make changes to python code, you must restart R

# make changes to index mappings by modifying inst/extdata/testdbas_index_mappings.json

source("tests/kibanaIntegrationTests/helper.R")

aliasExampleDocs <- "ntsp_dbas_upb"
newAliasName <- "ntsp_testdbas_upb"

exampleDocs <- getExampleDocs(aliasExampleDocs)
newDocs <- changeAliasInDocs(exampleDocs, newAliasName)

newDocs <- removeEicAndSpectra(newDocs)
newDocs <- addIntStdToDocs(newDocs)

indexName <- ingestNewDocs(newDocs)
stop()

# Go to Dashboard Time series SPM (integrationTests) 
# https://kibana02.dmz.bafg.de/s/ntsportal-dev/app/dashboards#/view/f6eb3fef-c349-44f9-9a34-70dc3c1a3bb8

elastic::index_delete(escon, indexName[[1]][[newAliasName]])


