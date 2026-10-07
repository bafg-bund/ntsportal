library(ntsportal)
library(tibble)
library(purrr)
msr <- "ntsp26.1_msrawfiles"
rootDirMsrawfiles <- "/beegfs/nts/ntsportal/msrawfiles"

allFilesMsrawfiles <- getTableByQuery(msr, fields = "path")$path
allFilesDir <- list.files(rootDirMsrawfiles, pattern = "\\.mzX?ML$", recursive = TRUE, full.names = TRUE)
filesNotFound <- setdiff(allFilesDir, allFilesMsrawfiles)
batchesNotFound <- unique(dirname(filesNotFound))
df <- tapply(filesNotFound, dirname(filesNotFound), \(x) tibble_row(batchname = unique(dirname(x)), num_files = length(x), filenames = paste(x, collapse = ", ")), simplify = FALSE) |> 
  array2DF() |> as_tibble() |> select(batchname, num_files, filenames)
View(df)
