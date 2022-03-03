dist_make_parallel <- function(x, distance_fcn, numCores, ...) 
{ 
  browser()
  distance_from_idxs <- function(idxs) {
    i1 <- idxs[1]
    i2 <- idxs[2]
    distance_fcn(x[i1, ], x[i2, ], ...)
  }
  size <- nrow(x)
  clf <- parallel::makeForkCluster(numCores)
  d <- parallel::parApply(clf, utils::combn(size, 2), 2, distance_from_idxs)
  parallel::stopCluster(clf)
  attr(d, "Size") <- size
  xnames <- rownames(x)
  if (!is.null(xnames)) {
    attr(d, "Labels") <- xnames
  }
  attr(d, "Diag") <- FALSE
  attr(d, "Upper") <- FALSE
  class(d) <- "dist"
  d
}