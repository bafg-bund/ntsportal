# Screening for non-existent batches returns a helpful error

    Code
      try(screeningSelectedBatches(testIndexName, "foo", "bar"))
    Message
      bar already exists, files will be overwritten
    Condition
      Warning in `warnNonExistentDirs()`:
      The following directories do not exist: foo
    Output
      Error in getSelectedMsrawfileBatches(msrawfileIndex, batchDirs, screeningType) : 
        No batches found in dir foo

