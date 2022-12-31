library(Matrix)

message("started clustering on date ", date())
pairs <- readRDS("~/temp/ucid_obj_pairs.RDS")
#allUfids <- readRDS("~/temp/ucid_obj_allUfids.RDS") 
allUfids <- c(pairs[, 1], pairs[, 2])
#pairs[,3] <- 1 - pairs[,3]

pairsSparse <- sparseMatrix(i = pairs[,1], j = pairs[,2], x = pairs[,3], dims=list(max(allUfids),max(allUfids)), symmetric = T)

pairsSparse <- 1 - pairsSparse
pairsSparse <- pairsSparse[1:2400,1:2400]
nn <- dbscan::frNN(pairsSparse, eps = 0.2)


clus <- dbscan::dbscan(pairsSparse, 0.2, 2)

clus

message("finished clustering on date ", date())

saveRDS(clus, "~/temp/ucid_obj_clus.RDS")
