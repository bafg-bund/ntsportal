library(logger)
library(ntsportal)
library(glue)
log_info("started")
newTableId <-ingestFeatureRecords("tests/longTestsScreening/testResults-dbasRheinKobPosParallel")
log_info("ended")
index <- newTableId[[1]]$ntsp26.1_feature_bfg 
View(getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec')))
getNrow(db, index)  # 7696
deleteTable(getDbComm(), index)
