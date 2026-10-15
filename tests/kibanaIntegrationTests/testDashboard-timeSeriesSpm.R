
# Test dashboard "Time Series SPM"

# Note: If you make changes to python code, you must restart R

source("tests/kibanaIntegrationTests/helper.R")

aliasExampleDocs <- "ntsp26.1_feature_upb"
newAliasName <- "ntsp26.1_feature_upb_kibana_integration_test"

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
deleteTable(getDbComm(), indexName[[1]][[newAliasName]])
