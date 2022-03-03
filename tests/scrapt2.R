mzres <- elastic::Search(
  escon, index, body =
    sprintf('
    {
      "query": {
        "match_all": {}
      },
      "size": 0,
      "aggs": {
        "ufids": {
          "terms": {
            "field": "%s",
            "size": 100000,
            "order": {
              "_key": "asc"
            }
          },
          "aggs": {
            "mz": {
              "avg": {
                "field": "mz"
              }
            }
          }
        }
      }
    }
    ', ufidType))
stopifnot(mzres$aggregations$ufids$sum_other_doc_count == 0)
mzc <- mzres$aggregations$ufids$buckets
allUfids <- vapply(mzc, "[[", numeric(1), i = "key")
ufidMzs <- vapply(mzc, function(x) x$mz$value, numeric(1))

# make rts contiguous so that you can search rtm by index rather than name (much faster)
df <- data.frame(ufid = allUfids, mz = ufidMzs)
nums <- data.frame(ufid = seq_len(max(allUfids)))
df2 <- merge(nums, df, all.x = T, by = "ufid")
df2[is.na(df2$mz), "rt"] <- 0

mzd <- parallelDist::parDist(as.matrix(df2$mz), method = "euclidean", threads = 6)
structure(mzd)
message("size of rt dist object: ", format(object.size(mzd), units = "MB", standard = "SI"))
nn <- length(mzd)

mzd2 <- ifelse(abs(21.98194 - mzd) <= 0.01 & rtd < rtlim, TRUE, FALSE)
sum(mzd2)
rowcol<-function(ix,n) { #given index, return row and column
  nr=ceiling(n-(1+sqrt(1+4*(n^2-n-2*ix)))/2)
  nc=n-(2*n-nr+1)*nr/2+ix+nr
  cbind(nr,nc)
}
lapply(which(mzd2), rowcol, n = nn)

