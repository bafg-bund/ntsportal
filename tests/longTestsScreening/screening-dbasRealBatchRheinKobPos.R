

# nohup Rscript tests/longTestsScreening/screening-dbasRealBatchRheinKobPos.R &> tests/longTestsScreening/testResultsSerial/rheinKobPos.log &
library(ntsportal)
tempSaveDir <- "tests/longTestsScreening/testResults/"
index <- "ntsp25.2_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/"
connectNtsportal()
dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir)

