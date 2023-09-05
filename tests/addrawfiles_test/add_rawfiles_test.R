source("~/connect-ntsp.R")
rfindex <- "g2_msrawfiles"
templateId <- find_templateid(escon, rfindex, polarity = "neg", station = "saale_wettin_m")

newPaths <- "~/Messdaten/00_to_sort/20230831_SPM/pos/Wett_08_2_pos.mzXML"
#newPaths <- list.files("~/Messdaten/zehren/schwebstoff/elz_neg/", 
#                       full.names = T, pattern = "Zeh_\\d{2}_\\d.*\\.mzXML$")
#newPaths <- list.files("~/Messdaten/weil/schwebstoff/rhw_neg/", 
#                       full.names = T, pattern = "W\\d{2}_neg\\d.*\\.mzXML$")
newPaths


#debug(add_rawfiles)
#isdebugged(add_rawfiles)
#undebug(add_rawfiles)

add_rawfiles(escon, rfindex, templateId, newPaths)



