source("~/connect-ntsp.R")
library(ntsportal)
rfindex <- "g2_msrawfiles"
templateId <- find_templateid(escon, rfindex, polarity = "pos", station = "rhein_ko_l", isBlank = F)
templateId <- "i_fZ34oBis-yLiJmYsvq"


#newPaths <- "~/Messdaten/00_to_sort/20230831_SPM/pos/Wett_08_2_pos.mzXML"
#newPaths <- list.files("~/Messdaten/zehren/schwebstoff/elz_neg/", 
#                       full.names = T, pattern = "Zeh_\\d{2}_\\d.*\\.mzXML$")
#newPaths <- list.files("~/Messdaten/weil/schwebstoff/rhw_neg/", 
#                       full.names = T, pattern = "W\\d{2}_neg\\d.*\\.mzXML$")
#newPaths <- list.files("~/messdaten/mosel/wasser/expn_tm_pos/2210", pattern = "KOMO", f= T)
newPaths <- list.files("~/messdaten/frame/neg/", pattern = "mzXML$", f= T)



#debug(add_rawfiles)
#isdebugged(add_rawfiles)
#undebug(add_rawfiles)

add_rawfiles(escon, rfindex, templateId, newPaths)



