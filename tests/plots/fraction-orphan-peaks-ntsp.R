

# Graphic to display all features and the number of features with ufids

library(ggplot2)
library(cowplot)
source("~/connect-ntsp.R")

indices <- c(
  "g2_nts*",
  "g2_nts_bfg",
  "g2_nts_upb",
  "g2_nts_lanuv",
  "g2_nts_expn"
)

orphan_frac <- function(ind) {
  res1 <- elastic::Search(escon, ind, size = 0, body = '
               {
  "query": {
    "match_all": {}
  }
}
               ')
  
  totalNum <- res1$hits$total$value
  
  
  res2 <- elastic::Search(escon, ind, size = 0, body = '
{
  "query": {
    "exists": {
      "field": "ufid"
    }
  }
}
')
  
  numWithUfid <- res2$hits$total$value
  
  df <- data.frame(category = c("totalNum", "numWithUfid"), value = c(totalNum, numWithUfid))
  ggplot(df, aes(category, value)) + geom_col() + ggtitle(ind)
}

plots <- lapply(indices, orphan_frac)
plot_grid(plotlist = plots)
