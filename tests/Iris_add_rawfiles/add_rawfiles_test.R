

library(ntsportal)

source("~/connect-ntsp.R")

path <- list.files(path = "~/messdaten_z/wettin/schwebstoff/saw_pos/", pattern = "^Wett.*mzXML$", full.names = T)

templateId <- find_templateid(escon, rfindex = "g2_msrawfiles" , polarity = "pos", station = "saale_wettin_m")

add_rawfiles(escon = escon, 
             rfindex = "g2_msrawfiles", 
             templateId = templateId, 
             newPaths = path 
             )

#Blank

path <- list.files(path = "~/messdaten_z/wettin/schwebstoff/saw_pos/", pattern = "^BW.*mzXML$", full.names = T)

templateId <- find_templateid(escon, rfindex = "g2_msrawfiles" , isBlank = T, polarity = "pos", station = "saale_wettin_m")

add_rawfiles(escon = escon, 
             rfindex = "g2_msrawfiles", 
             templateId = templateId, 
             newPaths = path 
)
