code <- function(){
  rm(list = ls())

  library(dplyr)
  library(lubridate)
  library(tidyr)
    
source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_incobisql_query_data.R")

inputsqlpath <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Models/UMMs_SQL/FR Nuke AvCap.sql"

StartDate<-lubridate::ceiling_date(now(),unit="days")
EndDate<-max(StartDate+lubridate::days(5),lubridate::ceiling_date(now(),unit="month"))

getData <- sqlGetQueryReultsInCo(databaseName = "InCo",
                                 sqlPath = inputsqlpath,
                                 inputdate_1 = StartDate,
                                 inputdate_2=EndDate,
                                 username = "sa_ForwardPowerTrading",
                                 password = "sVXTFQrQS3p8",
                                 author = "CPE",
                                 solution = "FR Nuke EPSI backtest v2",
                                 team = "LT")

datamani<-getData

datamani<-getData %>% 
  dplyr::rename(                
                 "137316_hour"="BELLEVILLE 1"
                ,"137317_hour"="BELLEVILLE 2" 
                ,"137318_hour"="BLAYAIS 1"    
                ,"137319_hour"="BLAYAIS 2"   
                ,"137320_hour"="BLAYAIS 3"   
                ,"137321_hour"="BLAYAIS 4"   
                ,"137322_hour"="BUGEY 2"     
                ,"137323_hour"="BUGEY 3"     
                ,"137324_hour"="BUGEY 4"      
                ,"137325_hour"= "BUGEY 5"    
                ,"137326_hour"= "CATTENOM 1"  
                ,"137327_hour"= "CATTENOM 2"   
                ,"137328_hour"= "CATTENOM 3"   
                ,"137329_hour"= "CATTENOM 4"  
                ,"137330_hour"= "CHINON 1"     
                ,"137331_hour"= "CHINON 2"     
                ,"137332_hour"= "CHINON 3"    
                ,"137333_hour"= "CHINON 4"    
                ,"137334_hour"= "CHOOZ 1"      
                ,"137335_hour"="CHOOZ 2"      
                ,"137336_hour"="CIVAUX 1"     
                ,"137337_hour"="CIVAUX 2"     
                ,"137338_hour"="CRUAS 1"      
                ,"137339_hour"="CRUAS 2"      
                ,"137340_hour"="CRUAS 3"      
                ,"137341_hour"="CRUAS 4"      
                ,"137342_hour"="DAMPIERRE 1"  
                ,"137343_hour"="DAMPIERRE 2"  
                ,"137344_hour"="DAMPIERRE 3"  
                ,"137345_hour"="DAMPIERRE 4"  
                ,"137346_hour"="FLAMANVILLE 1" 
                ,"137347_hour"="FLAMANVILLE 2" 
                ,"137348_hour"="GOLFECH 1"     
                ,"137349_hour"="GOLFECH 2"     
                ,"137350_hour"="GRAVELINES 1"  
                ,"137351_hour"="GRAVELINES 2"  
                ,"137352_hour"="GRAVELINES 3"  
                ,"137353_hour"="GRAVELINES 4"  
                ,"137354_hour"="GRAVELINES 5" 
                ,"137355_hour"= "GRAVELINES 6" 
                ,"137356_hour"= "NOGENT 1"     
                ,"137357_hour"= "NOGENT 2"     
                ,"137358_hour"= "PALUEL 1"     
                ,"137359_hour"= "PALUEL 2"     
                ,"137360_hour"= "PALUEL 3"     
                ,"137361_hour"= "PALUEL 4"     
                ,"137362_hour"= "PENLY 1"      
                ,"137363_hour"= "PENLY 2"     
                ,"137364_hour"= "ST LAURENT 1"  
                ,"137365_hour"= "ST LAURENT 2" 
                ,"137366_hour"= "ST ALBAN 1"   
                ,"137367_hour"= "ST ALBAN 2"   
                ,"137368_hour"= "TRICASTIN 1"   
                ,"137369_hour"= "TRICASTIN 2"  
                ,"137370_hour"= "TRICASTIN 3"  
                ,"137371_hour"= "TRICASTIN 4"  
  )

datamani$CETCESTHour<-as.POSIXct(sub("'","",datamani$CETCESTHour))
names(datamani)[names(datamani)=="CETCESTHour"]<-"ValueDate"
datamani$ValueDate<-with_tz(as.POSIXct(sub("'","",datamani$ValueDate)),"UTC")

savepath <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/csv/"
filename <- "FR_NUKE_AVcap_upload.csv"

data.table::fwrite(datamani,paste0(savepath,filename),sep=";",dec = ".", row.names = F, quote = F, dateTimeAs = "write.csv")

uploadpath <- "https://epsi.it-prod.incom.as/upload-csv"

httr::POST(uploadpath, body = httr::upload_file(paste0(savepath,filename)))

}

logname = "FR_Nuke_AvCap_EPSIupload"
source("//172.17.0.6/Shared\\Analysis\\CodeLibrary\\R\\logfun.R")
logfun(code, logname, url = "https://hooks.slack.com/services/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", hourssincelast = 0)
