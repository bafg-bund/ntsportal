# display ufid spectra
library(ggplot2)
library(dplyr)
tbl(udb, "feature") %>% collect() %>% View()
tbl(udb, "ms2") %>% collect() %>% View()

tbl(udb, "feature") %>% collect() %>% .$mz

tbl(udb, "feature") %>% filter(ufid == 8) %>% select(mz) %>% collect() %>% .$mz

tbl(udb, "retention_time") %>% collect()

tbl(udb, "retention_time") %>% filter(ufid == 2) %>% select(rt) %>% collect() %>% .$rt

ms1 <- tbl(udb, "ms1") %>% filter(ufid == 24) %>% collect()

ggplot(ms1, aes(x = mz, xend = mz, y = 0, yend = rel_int)) +
  geom_segment() +
  geom_text(aes(y = rel_int, label = round(mz, 4)), subset(ms1, rel_int > .1), vjust=-.1)


ms2 <- tbl(udb, "ms2") %>% filter(ufid == 24) %>% collect()

ggplot(ms2, aes(x = mz, xend = mz, y = 0, yend = rel_int)) +
  geom_segment() +
  geom_text(aes(y = rel_int, label = round(mz, 4)), subset(ms2, rel_int > .01), vjust=-.1)

udb_clean_spectra(udb, 2, "ms2")


# create scatter plots

# get all filenames

x <- elastic::Search(escon, index, body = '
                {
  "query": {
    "match_all": {}
  },
  "size": 0,
  "aggs": {
    "filenames": {
      "terms": {
        "field": "filename",
        "size": 1000
      }
    }
  }
}
                ')
filenames <- sapply(x$aggregations$filenames$buckets, function(y) y$key)

get_dots <- function(fn) {
  res <- elastic::Search(escon, index, body = sprintf('
{
  "query": {
    "term": {
      "filename": {
        "value": "%s"
      }
    }
  },
  "size" : 10000,
  "_source": ["mz", "rt", "area", "start"]
}
                ', fn))
  data.frame(mz = sapply(res$hits$hits, function(x) x$`_source`$mz),
             rt = sapply(res$hits$hits, function(x) x$`_source`$rt),
             area = sapply(res$hits$hits, function(x) x$`_source`$area),
             start = sapply(res$hits$hits, function(x) x$`_source`$start))
}

pls <- lapply(filenames, get_dots)
#max(Reduce(rbind, pls)$mz)
starts <- sapply(pls, function(y) y$start[1])
starts <- lubridate::ymd(starts)
pls <- pls[order(starts)]
dir.create("tests/scatter_plot_film")
create_scatter <- function(df) {
  ggplot(df, aes(rt, mz, color = area)) +
    geom_point(alpha = .8) +
    scale_x_continuous(limits = c(2,20)) +
    scale_y_continuous(limits = c(100,1000)) +
    scale_color_steps(trans = "log10", low = "steelblue", high = "black", limits = c(10,60000)) +
    #scale_size_binned(trans = "log10") +
    xlab("Retentionszeit / Min.") + ylab("m/z / Da") + labs(color = "Intensität") +
    ggtitle(df$start[1])
}

i <- 1
create_scatter(pls[[i]])
ggsave(paste0("tests/scatter_plot_film/image-", i, ".jpg"), height = 1000, width = 2000, units = "px")


for (i in seq_along(pls)) {
  create_scatter(pls[[i]])
  ggsave(paste0("tests/scatter_plot_film/image-", i, ".jpg"), height = 1000, width = 2000, units = "px")
}

