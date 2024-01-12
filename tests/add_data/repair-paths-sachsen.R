

# correct path names Sachsen...

library(elastic)
RFINDEX <- "g2_msrawfiles"
source("~/connect-ntsp.R")

res <- Search(escon, RFINDEX, size = 10000, body = '{
  "query": {
    "regexp": {
      "path": ".*HRMS/NTS_Sachsen.*"
    }
  },
  "_source": ["path"]
}')

h <- res$hits$hits

oldPaths <- sapply(h, function(x) x[["_source"]]$path)
fileNames <- basename(oldPaths)

# fileNames <- fileNames[1:3]
# oldPaths <- oldPaths[1:3]
# Find files in Messdaten

newPaths <- vapply(fileNames, function(x) {
  r <- normalizePath(list.files("~/messdaten/sachsen/", pattern = x, recursive = T, f = T))
  
  if (length(r) == 0) return("not found")
  if (length(r) > 1) return(paste(r, collapse = ", "))
  
  r
}
, character(1))
sum(newPaths == "not found")

df <- data.frame(
  old = oldPaths,
  new = newPaths,
  filename = fileNames
)


prep <- sprintf('cmp --silent %s %s || echo "files are different"', df[1, "new"], df[1, "old"])
symRes <- system(prep, intern = T)

sames <- sapply(seq_len(nrow(df)), function(x) {
  file.size(df[x, "old"]) == file.size(df[x, "new"])   
})
all(sames)
sames <- sapply(seq_len(nrow(df)), function(x) {
  tools::md5sum(df[x, "old"]) == tools::md5sum(df[x, "new"])   
})
all(sames)

numb <- sapply(seq_len(nrow(df)), function(x) {
  qb <- list(
    term = list(
      filename = df[x, "filename"]
    )
  )
  res <- Search(escon, RFINDEX, body = list(query = qb), size = 0)
  res$hits$total$value
})
table(numb)
df[which(numb == 3),]

numb <- sapply(seq_len(nrow(df)), function(x) {
  qb <- list(
    term = list(
      path = df[x, "old"]
    )
  )
  res <- Search(escon, RFINDEX, body = list(query = qb), size = 0)
  res$hits$total$value
})
table(numb)
df[which(numb == 2),]
for (i in 555:nrow(df)) { # i <- 1
  qb <- list(
    term = list(
      filename = df[i, "filename"]
    )
  )
  res <- Search(escon, RFINDEX, body = list(query = qb), size = 0)
  stopifnot(res$hits$total$value == 1)
  tryCatch(
    res <- docs_update_by_query(
      escon, RFINDEX, 
      body = list(
        query = qb,
        script = list(
          source = "ctx._source.path = params.newPath",
          params = list(
            newPath = df[i, "new"]
          )
        )
      )
    ),
    error = function(cnd) {
      message("failed ", df[i, "old"])
    }
  )
}



