x_func_preprocessing_dashboard_data <- function(data = NULL){
  dash_data <- data %>% 
    select( c(Name=X_source.name, # if available
              Formula=X_source.formula, 
              CAS_RN=X_source.cas, 
              Matrix=X_source.matrix, 
              Intensity=X_source.intensity, 
              X_source.rtt, # user needs to select rtt.method with Chrom. Method filter
              Area=X_source.area, 
              Chrom_Meth=X_source.chrom_method,
              Method=X_source.chrom_method,
              Ufid=X_source.ufid, # rm NA's , if available
              mz=X_source.mz, # average mz
              Classification=X_source.comp_group, # rm NA's, as comma sep list
              Area_normalized=X_source.area_normalized,
              Stations=X_source.station,
              lat=X_source.loc.lat,
              lon=X_source.loc.lon,
              Time=X_source.start,
              River=X_source.river,
              tRet=X_source.rt
    )) %>% 
    mutate(
      Intensity=ifelse(is.na(Intensity),0,Intensity),
    ) %>%
    # unnest(X_source.rtt) %>%
    # rename(tRet=rt,
    #        Method=method) %>%
    mutate(location=paste0(lat,lon))
  return(dash_data)
}





# x_func_preprocessing_dashboard_data_ui <- function(data = NULL){
# 
#   temp_data <- data
#   #temp_data <- temp_data %>% unnest(X_source.rtt)
#   param_data <- list()
#   param_data$Stations <- list(na.omit(unique(temp_data$X_source.station)))
#   param_data$River <- list(na.omit(unique(temp_data$X_source.river)))
#   param_data$matrix <- list(na.omit(unique(temp_data$X_source.matrix)))
#   param_data$tag <- list(na.omit(unique(temp_data$X_source.tag)))
#   param_data$comp_group <- list(na.omit(unique(unlist(temp_data$X_source.comp_group))))
#   #param_data$rtt_method <- list(na.omit(unique(temp_data$method)))
#   param_data$rtt_method <- list(na.omit(unique(temp_data$X_source.chrom_method)))
#   param_data$name <- list(na.omit(unique(unlist(temp_data$X_source.name))))
#   param_data$Ufid <- list(na.omit(unique(temp_data$X_source.ufid)))
#   
#   param_data$rt_min_max <- c(min(temp_data$X_source.rt), max(temp_data$X_source.rt))
#   param_data$mz_min_max <- c(min(temp_data$X_source.mz), max(temp_data$X_source.mz))
#   
#   
#   
#   rm(temp_data)
#   return(param_data)
# }




#