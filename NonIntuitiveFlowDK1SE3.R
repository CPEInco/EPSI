source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_incobisql_query_data.R")
source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_lilac_query_data.R")

inputsqlpath <- "NordicFlows+Cap.sql"
inputsqlpath2<-"Cap_reduction_DK1v2.sql"

options(digits=9)

getData <- sqlGetQueryReultsInCo(databaseName = "InCo",
                                 sqlPath = inputsqlpath,
                                 username = "sa_ForwardPowerTrading",
                                 password = "sVXTFQrQS3p8",
                                 author = "CPE",
                                 solution = "FR Nuke EPSI backtest v2",
                                 team = "LT")

getData2 <- sqlGetQueryReults(databaseName = "FDMData",
                                 sqlPath = inputsqlpath2,
                                 UserName = "cpe@in-commodities.com",
                                 Password = "B@eN8Mbv=GWq")
datamani2<-getData2 %>% 
  dplyr::mutate(ValueDateUTC=force_tz(ValueDate, tz="UTC")) %>% 
  dplyr::select(-c(ValueDate))


datamani<-getData %>%
  dplyr::left_join(datamani2) %>% 
  dplyr::mutate(difference=NO1-DK1,
                NonIntuitive_forecast_6=ifelse(DK1-SE3>0.01 & difference>0.01 & SE3-NO1<(-5),1,0))

datamani<-getData %>% 
  dplyr::left_join(datamani2) %>% 
  #dplyr::select(-c(`DK1-NO2-CAP`,`NO2-DK1-CAP`,`NO1-SE3-CAP`,`SE3-NO1-CAP`,`DK1-NO2-FLOW`,`NO2-DK1-FLOW`,`NO2 Price`,`DK1-NO1->SE3-CAP`)) %>% 
  dplyr::mutate(NonIntuitive_actual=ifelse(`DK1-SE3-FLOW`>0 & `DK1 Price`>`SE3 Price`,1,0),
                DateTimeCET=with_tz(ValueDateUTC, tz="CET"),
                difference=NO1-DK1,
                difference_PC=`NO1 PC`-`DK1 PC`,
                IsWeekdayPEAK=ifelse(!weekdays(ValueDateUTC) %in% c("Saturday", "Sunday") & format(DateTimeCET, "%H") %in% c("06","07","08","09","10", "18","19", "20","21"),0,1),
                IsPEAK=ifelse(format(DateTimeCET, "%H") %in% c("06","07","08","09", "18","19", "20"),0,1),
                Time=format(DateTimeCET, "%H"),
                NonIntuitive_forecast_6=ifelse(DK1-SE3>0.01 & difference>0.01 & SE3-NO1<(-0.01),1,0),
                NonIntuitive_forecast_5=ifelse(DK1-SE3>0.01 & difference>0.01 & SE3-NO1<(-0.01) & IsWeekdayPEAK==1,1,0),
                NonIntuitive_forecast_1=ifelse(`DK1 PC`-`SE3 PC`>0 & difference_PC>0 & `SE3 PC`-`NO1 PC`<0 & IsWeekdayPEAK==1,1,0),
                NonIntuitive_forecast_2=ifelse(`DK1 PC`-`SE3 PC`>0 & difference_PC>0 & `SE3 PC`-`NO1 PC`<0,1,0),
                NonIntuitive_forecast_3=ifelse(DK1>SE3 & `SE3->DK1-NO1-CAP`<`DK1-SE3-CAP` & DK1-NO1<(-0.3),1,0),
                NonIntuitive_forecast_4=ifelse(DK1-SE3>5 & `SE3->DK1-NO1-CAP`<`DK1-SE3-CAP` & DK1-NO1<(-0.3),1,0),
                
                Correct_6=ifelse(NonIntuitive_forecast_6==NonIntuitive_actual,1,0),
                Correct_5=ifelse(NonIntuitive_forecast_5==NonIntuitive_actual,1,0),
                Correct_4=ifelse(NonIntuitive_forecast_4==NonIntuitive_actual,1,0),
                Correct_3=ifelse(NonIntuitive_forecast_3==NonIntuitive_actual,1,0),
                Correct_2=ifelse(NonIntuitive_forecast_2==NonIntuitive_actual,1,0),
                Correct_1=ifelse(NonIntuitive_forecast_1==NonIntuitive_actual,1,0))

datamani$Correct_1<-zoo::na.locf(datamani$Correct_1)
datamani$Correct_2<-zoo::na.locf(datamani$Correct_2)
datamani$Correct_3<-zoo::na.locf(datamani$Correct_3)
datamani$Correct_4<-zoo::na.locf(datamani$Correct_4)
datamani$Correct_5<-zoo::na.locf(datamani$Correct_5)
datamani$Correct_6<-zoo::na.locf(datamani$Correct_6)

print(paste0("Hit rate forecast 1: ",sum(sum(datamani$Correct_1)/nrow(datamani))))
print(paste0("Hit rate forecast 2: ",sum(sum(datamani$Correct_2)/nrow(datamani))))
print(paste0("Hit rate forecast 3: ",sum(sum(datamani$Correct_3)/nrow(datamani))))
print(paste0("Hit rate forecast 4: ",sum(sum(datamani$Correct_4)/nrow(datamani))))
print(paste0("Hit rate forecast 5: ",sum(sum(datamani$Correct_5)/nrow(datamani))))
print(paste0("Hit rate forecast 6: ",sum(sum(datamani$Correct_6)/nrow(datamani))))

NonIntuitiveFlowForecast<-datamani %>% 
  dplyr::filter(NonIntuitive_forecast_6==1) %>%
  dplyr::mutate(FlowForecast=ifelse(NO1-DK1>0.02,`SE3-NO1-CAP`-`SE3->DK1-NO1-CAP`,0))

