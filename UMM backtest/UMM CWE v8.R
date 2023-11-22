##### LAST RUN 22/11-2023 ######

rm(list = ls())

library(lubridate)
library(readxl)
library(dplyr)
library(xml2)
library(XML)
library(AzureStor)
library(readxl)

#IDS_DB<-read.csv(file="masterdata_epsi_own_avcap.csv",sep=";",header=T) %>% 
IDS_DB<-read.csv(file="//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/Backtestv2/Availability/masterdata_epsi_own_avcap.csv",sep=";",header=T) %>% 
  dplyr::transmute(Curveid=SAS.ID
                   ,`Plant name`=Unit)



readXMLandFormat <- function(urlstring){

  options(timeout = 120)
  
  download.file(urlstring,destfile="updates.xml")
  tt <-xmlParse(file="updates.xml")
  

  out <- XML::getNodeSet(tt, "//*[name()='update']", fun=XML::xmlToList)
  
  dfout <- data.frame(do.call(rbind, out))
  
  df <- dfout %>% 
    dplyr::mutate(updatename=uri_name,
                  ForecastDateUTC=ISOdatetime(year = substring(ts,1,4), 
                                              month = substring(ts,6,7),
                                              day = substring(ts,9,10),
                                              hour = as.numeric(substring(ts,12,13)),
                                              min = substring(ts,15,16),
                                              sec = 0,
                                              tz = "UTC"),
                  
                  ForecastDateCET=as.POSIXlt(format(ForecastDateUTC, tz="CET"))) 
  
  return(df)
}

readXMLandFormat2 <- function(urlstring){

  download.file(urlstring,destfile="updates.xml")
  tt <-xmlParse(file="updates.xml")
  
  out <- XML::getNodeSet(tt, "//*[name()='value']", fun=XML::xmlToList, addFinalizer = FALSE)
  
  dfout <- data.frame(do.call(rbind, out))
  df1 <- data.frame(do.call(rbind, dfout[,1]))
  df2 <- data.frame(do.call(rbind, dfout[,2]))
  
  df <- bind_cols(df2, df1)
  
  colnames(df) <- c("ValueDate", "Value")
  
  df$ValueDateUTC <- ISOdatetime(year = substring(df$ValueDate,1,4), 
                                 month = substring(df$ValueDate,6,7),
                                 day = substring(df$ValueDate,9,10),
                                 hour = as.numeric(substring(df$ValueDate,12,13)),
                                 min = substring(df$ValueDate,15,16),
                                 sec = 0,
                                 tz = "UTC")
  df<-df %>%
    dplyr::select(ValueDateUTC, Value)
  
  return(df)
}                  

#Azure storage
account_endpoint <- "https://incolocaldatalake.blob.core.windows.net"
account_key <- "Mfo1WEABdm9LLGK981Kr4NJ8WlVLaDoK1Ze1xFwoSE1Bzw0dhjCJKJznWVWLonRv5QBqYQlsA987+AStIo0zuQ=="
container_name <- "forwardpower"
bl_endp_key <- storage_endpoint(account_endpoint, key=account_key)
cont <- storage_container(bl_endp_key, container_name)

conn<- RODBC::odbcConnect("DatabricksPOC_DSN")

.rs.restartR()

for (i in 1:nrow(IDS_DB)){
  
  #Get all Forecast dates for a given curveid allready in database
  UpdatesDb<-RODBC::sqlQuery(conn,paste0("SELECT DISTINCT(ForecastDateUTC) FROM pub.epsi_umm_optimized where curveid='",IDS_DB$Curveid[i],"' order by ForecastDateUTC desc" )) %>% 
    dplyr::transmute(
      AlreadyInDb=1,
      ForecastDateUTC=force_tz(ForecastDateUTC, tzone="UTC"))
      
  #Get all forecastdates for a given curveid
  CurveUpdates <- readXMLandFormat(urlstring = paste0("https://epsi.genscape.com/export/download_sas_updates_list_xml?account=216&id=",IDS_DB$Curveid[i],"&password=Cd65V21X")) 
  
  if (nrow(CurveUpdates)>0) {
    
    df<-CurveUpdates %>% 
      dplyr::left_join(UpdatesDb, by=c("ForecastDateUTC"="ForecastDateUTC")) %>% 
      dplyr::mutate(AlreadyInDb=ifelse(is.na(AlreadyInDb), 0,AlreadyInDb)
                    ,EndDate=as.Date(ForecastDateUTC)+as.numeric(as.Date(lead(ForecastDateUTC))-as.Date(ForecastDateUTC))+35
                    ,ForecastDateCET=with_tz(ForecastDateUTC,tzone="CET"))
                    
    df$EndDate[is.na(df$EndDate)] <- as.Date(Sys.Date()+lubridate::days(120))                  
    
    x<-nrow(df) 
    
       df <- df %>%
        dplyr::mutate(
        next_9am = case_when(
          hour(ForecastDateCET) >= 9 ~ ceiling_date(ForecastDateCET, "day") + hours(9),
          TRUE ~ floor_date(ForecastDateCET, "day") + hours(9)
        ),
        hours_to_next_9am = as.numeric(difftime(next_9am, ForecastDateCET, units = "hours"))) %>% 
      dplyr::group_by(next_9am) %>%
      dplyr::filter(hours_to_next_9am == min(hours_to_next_9am) & AlreadyInDb==0) %>%
      ungroup()

    
  print(paste0("Original columns: ", x, " Downloading columns: ", nrow(df)))
    
  if(nrow(df)>0)
  
    {  
    for (j in 1:nrow(df)){
      
      tryCatch(
        
        {Curves<-readXMLandFormat2(urlstring = paste0("https://epsi.genscape.com/export/download_sas_xml?account=216&id=",IDS_DB$Curveid[i],"&update=",df$updatename[j],"&end_date=",df$EndDate[j],"&password=Cd65V21X"))}
        ,
        error=function(e){
          Curves<-readXMLandFormat2(urlstring = paste0("https://epsi.genscape.com/export/download_sas_xml?account=216&id=",IDS_DB$Curveid[i],"&update=",df$updatename[j],"&password=Cd65V21X"))
        }
      )
      
      Curvesmani<-Curves
      Curvesmani$ForecastDateUTC<-format(df$ForecastDateUTC[j], '%Y-%m-%d %H:%M:%S')
      Curvesmani$CurveId<-paste0(IDS_DB$Curveid[i])
      Curvesmani$ScenarioId<-0
      Curvesmani$CsvCreatedDate<-Sys.time()
      
      w_con <- textConnection("foo", "w")
      write.csv(Curvesmani, w_con)
      r_con <- textConnection(textConnectionValue(w_con))
      close(w_con)
      upload_blob(cont, src=r_con, dest=  paste0("epsi/", Curvesmani$CurveId[1],"_",sub(" ","_",gsub(":","_",Curvesmani$ForecastDateUTC[1])),".csv"))
      close(r_con) 
      
      print(paste0("i: ", i," j: ",j,"/",nrow(df)))
      
    }
   }
  }       
}
