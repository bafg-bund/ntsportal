library(ntsportal)

dirTestResults <- "tests/slurmTesting/testResults-dbas-rheinKobPos"
# After completion
# Remember to delete indices after ingest
logger::log_info("started")
newTableId <- ingestFeatureRecords(dirTestResults)
logger::log_info("ended")

index <- newTableId[[1]]$ntsp26.1_feature_bfg 
View(getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec, duration')))
nrow(getTableByEsql(glue('FROM {index} | STATS BY month = BUCKET(start, "1 month")')))  # 2
getNrow(db, index)  # 7696
deleteTable(getDbComm(), index)

