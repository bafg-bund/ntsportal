

# Script create scatterplot

library(ggplot2)
library(plotly)
library(lubridate)
source("~/connect-ntsp.R")

# keep returning results until all have been captured
sortMz <- 0
sortRt <- 0
rm(fts)
repeat {
  res <- elastic::Search(escon, "g2_nts_actualmz", body = sprintf('
  {
  "search_after": [%.4f, %.2f],
  "sort": [
    {
      "mz": {
        "order": "asc"
      }
    },
    {
      "rt": {
        "order": "asc"
      }
    }
  ],
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "mz": {
              "gte": 200,
              "lte": 400
            }
          }
        },
        {
          "range": {
            "rt": {
              "gte": 4,
              "lte": 8
            }
          }
        }
      ]
    }
  },
  "size": 10000,
  "_source": [
    "mz",
    "rt",
    "area_normalized",
    "start",
    "ufid",
    "ms2"
  ]
}
', sortMz, sortRt))
  feats <- res$hits$hits
  #feats[[1]]
  if (length(feats) == 0){
    break 
  } else {
    sortMz <- feats[[length(feats)]]$sort[[1]]
    sortRt <- feats[[length(feats)]]$sort[[1]]
  }
  # some features do not have a ufid, value is null
  ufids <- lapply(feats, function(x) x[["_source"]]$ufid)
  ufids <- as.numeric(lapply(ufids, function(x) if (is.null(x)) 0 else x))
  ms2s <- lapply(feats, function(x) x[["_source"]]$ms2)
  ms2s <- as.numeric(lapply(ms2s, function(x) if (is.null(x)) 0 else 1))
  
  fts1 <- data.frame(
    mz = sapply(feats, function(x) x[["_source"]]$mz),
    rt = sapply(feats, function(x) x[["_source"]]$rt),
    an = sapply(feats, function(x) x[["_source"]]$area_normalized),
    year = as.factor(year(ymd(sapply(feats, function(x) x[["_source"]]$start)))),
    ufid = as.factor(ufids),
    ms2 = as.factor(ms2s),
    id = sapply(feats, "[[", i = "_id")
  )
  
  if (exists("fts"))
    fts <- rbind(fts, fts1) else fts <- fts1
}




plt <- ggplot(fts, aes(rt, mz, colour = year)) + 
  geom_point(alpha = .5, shape = 21) + 
  labs(colour = "Year", x = "Retention time", y = "m/z") +
  theme_bw()
plt
ggplotly(plt)

library(RColorBrewer)
numNeeded <- length(unique(fts$ufid))
qual_col_pals = brewer.pal.info[brewer.pal.info$category == 'qual',]
col_vector = unlist(mapply(brewer.pal, qual_col_pals$maxcolors, rownames(qual_col_pals)))
colSet <- brewer.pal(12, "Paired")
# there are 19 shapes in total
colSetrep <- rep(colSet, each = 19)
shapeRep <- rep(1:19, length(colSet))
set.seed(1000)
symb <- data.frame(
  color = colSetrep[sample(seq_along(colSetrep), numNeeded, replace = T)],
  shape = shapeRep[sample(seq_along(shapeRep), numNeeded, replace = T)]
)

#geom_point(aes(rt, mz), subset(fts, ms2 == 1), alpha = 0.1, shape = 15, fill = "grey50", size = 4) +
  
plt2 <- ggplot() + 
  geom_point(aes(rt, mz, colour = ufid, shape = ufid, size = ms2, group = id), fts, alpha = .6) +
  scale_size_manual(values = c(2,3)) +
  scale_color_manual(values = symb$color) + 
  scale_shape_manual(values = symb$shape) +
  guides(color = "none", shape = "none") +
  labs(x = "Retention time", y = "m/z") +
  theme_bw()
plt2
ggplotly(plt2)
ggsave("tests/ufid_alignment/230517-actualmz-ufid-scatter.pdf", plot = plt2, width = 420, height = 297, units = "mm")
