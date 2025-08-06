



fileUrl <- "https://picture.ntsportal.bafg.de/FFBIRENCNBBTSF-UHFFFAOYSA-O.png"
filePathHd <- "/srv/cifs-mounts/ntsportal/intern/picture_sync/FFBIRENCNBBTSF-UHFFFAOYSA-O.png"
checkPngSyncronization(fileUrl, filePathHd)


fileUrl <- "https://picture.ntsportal.bafg.de/DCOPUUMXTXDBNB-UHFFFAOYSA-N.png"
filePathHd <- "/srv/cifs-mounts/ntsportal/intern/picture_sync/DCOPUUMXTXDBNB-UHFFFAOYSA-N.png"
checkPngSyncronization(fileUrl, filePathHd)

pthDb <- getSpectralLibraryPath("ntsp25.2_msrawfiles")
syncDir <- "/srv/cifs-mounts/ntsportal/intern/picture_sync"
checkPngAvailability(pthDb, syncDir)
removeRedundantPngs(pthDb, syncDir)
