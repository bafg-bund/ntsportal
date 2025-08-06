

# nohup Rscript tests/longTestsScreening/screening-dbasRealBatchRheinKobPosParallel.R &> tests/longTestsScreening/testResults/rheinKobPos.log &
library(ntsportal)
connectNtsportal()
tempSaveDir <- "tests/longTestsScreening/testResultsParallel/"
index <- "ntsp25.2_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)

dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2)

