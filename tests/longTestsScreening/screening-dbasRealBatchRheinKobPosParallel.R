

# nohup Rscript tests/longTestsScreening/screening-dbasRealBatchRheinKobPosParallel.R &> tests/longTestsScreening/testResults/rheinKobPos.log &

tempSaveDir <- "tests/longTestsScreening/testResults/"
index <- "ntsp_msrawfiles"
batchDirectory <- c(
  "/srv/cifs-mounts/g2/G/G2/HRMS/Messdaten/koblenz/wasser/2021/202105/pos/",
  "/srv/cifs-mounts/g2/G/G2/HRMS/Messdaten/koblenz/wasser/2021/202106/pos/"
)
ntsportal::connectNtsportal()
ntsportal::dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2)
warnings()
