library(logger)
library(ntsportal)
library(glue)
log_info("started")
newTableId <-ingestFeatureRecords("tests/longTestsScreening/testResults-dbasWeilMmp2023")
log_info("ended")
index <- newTableId[[1]]$ntsp26.1_feature_upb 
View(getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec, duration')))
nrow(getTableByEsql(glue('FROM {index} | STATS BY pol')))  # 2
getNrow(db, index)  # 4106
deleteTable(getDbComm(), index)
