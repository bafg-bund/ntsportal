

# nohup Rscript tests/longTestsScreening/screening-dbasRealBatchRheinKobPos.R &> tests/longTestsScreening/testResults/rheinKobPos.log &

tempSaveDir <- "tests/longTestsScreening/testResults/"
index <- "ntsp_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/"
ntsportal::connectNtsportal()
ntsportal::dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir)
warnings()
