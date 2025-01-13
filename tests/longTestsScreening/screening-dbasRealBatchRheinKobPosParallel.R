

# nohup Rscript tests/longTestsScreening/screening-dbasRealBatchRheinKobPosParallel.R &> tests/longTestsScreening/testResults/rheinKobPos.log &

tempSaveDir <- "tests/longTestsScreening/testResults/"
index <- "ntsp_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
ntsportal::connectNtsportal()
ntsportal::dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2)
warnings()
