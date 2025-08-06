

# Test dashboard "Time Series SPM"

# Note: If you make changes to python code, you must restart R

source("tests/kibanaIntegrationTests/helper.R")

aliasExampleDocs <- "ntsp_dbas_upb"
newAliasName <- "ntsp_2025_1dbas_upb"

# Process 2 Bimmen files for one compound

# ingest docs

exampleDocs <- getExampleDocs(aliasExampleDocs)
newDocs <- changeAliasInDocs(exampleDocs, newAliasName)

newDocs <- removeEicAndSpectra(newDocs)
newDocs <- addIntStdToDocs(newDocs)

indexName <- ingestNewDocs(newDocs)
stop()

# Go to Dashboard Time series SPM (integrationTests) 
# https://kibana02.dmz.bafg.de/s/ntsportal-dev/app/dashboards#/view/f6eb3fef-c349-44f9-9a34-70dc3c1a3bb8

elastic::index_delete(escon, indexName[[1]][[newAliasName]])


