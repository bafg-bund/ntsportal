# Screening for non-existent batches returns a helpful error

    Code
      try(screeningSelectedBatches(testIndexName, "foo", "bar"))
    Output
      Error in getSelectedMsrawfileBatches(msrawfileIndex, batchDirs) : 
        No batches found in dir foo

