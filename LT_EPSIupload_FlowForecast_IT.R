
library(dplyr)

token <- "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"
req <- httr::content(httr::GET("https://api.github.com/repos/incomas/LT.CodeLibrary/contents/R/load_get_data_functions.R", httr::authenticate(token, "")))
devtools::source_url(req$download_url)
rm(req, token)
load_get_data_sql_functions("get_inco_data","get_inco_query_data","get_lilac_data","get_lilac_query_data")

forecastDateTime <- lubridate::with_tz(as.POSIXct(now(), tz = "Europe/Paris"),"UTC")
startDateTime <-lubridate::with_tz(as.POSIXct(lubridate::ceiling_date(lubridate::now("Europe/Paris"),unit="days"), tz = "Europe/Paris"),"UTC")
endDateTime <- lubridate::with_tz(as.POSIXct(lubridate::ceiling_date(lubridate::now("Europe/Paris")+lubridate::days(15),unit="days"), tz = "Europe/Paris"),"UTC")

get_flowforecast <- function(forecastDateTime, startDateTime, endDateTime){
  
  if(forecastDateTime <= startDateTime) {
    startDateTimeInput <- startDateTime
    startDateTime <- lubridate::with_tz(lubridate::floor_date(as.POSIXct(as.character(forecastDateTime), tz = "Europe/Paris"), "month"),"UTC")
  }
  
  n <- 20
  
  ## GET FUNDAMENTALS DATA ####
  
  masterdatapath <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/masterdata/masterdata_flowforecast.csv"
  
  # List of priceareas fundamental data are needed.
  priceareas <- c("IT_CNOR", "IT_NOR")
  
  # Load masterdata
  masterdata <- data.table::fread(masterdatapath, sep = ";") %>% 
    dplyr::filter(PriceArea %in% priceareas) %>% 
    dplyr::mutate(Demand = as.character(Demand))
  
  list1 <- c("IT_NOR")
  list2 <- c("IT_aNOR")
  
  priceareas <- masterdata$PriceArea
  for(i in seq_along(list1)) priceareas <- gsub(list1[i], list2[i], priceareas, fixed = TRUE)
  
  masterdata$PriceArea <- priceareas
  
  # Split masterdata after datatype
  masterdata_demand <- masterdata %>% dplyr::select(PriceArea, Demand, View_demand)
  masterdata_wind <- masterdata %>% dplyr::select(PriceArea, Wind, View_wind) %>% dplyr::filter(Wind > 1000)
  masterdata_solar <- masterdata %>% dplyr::select(PriceArea, Solar, View_solar) %>% dplyr::filter(Solar > 1000)
  
  # Split masterdata after view
  masterdata_demand_ts1 <- masterdata_demand %>% dplyr::filter(View_demand == "TimeSeries1_v02") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_demand"))
  masterdata_demand_meteo <- masterdata_demand %>% dplyr::filter(View_demand == "Meteologica_TimeSeries") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_demand"))
  masterdata_wind_ts1 <- masterdata_wind %>% dplyr::filter(View_wind == "TimeSeries1_v02") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_wind"))
  masterdata_wind_meteo <- masterdata_wind %>% dplyr::filter(View_wind == "Meteologica_TimeSeries") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_wind"))
  masterdata_solar_ts1 <- masterdata_solar %>% dplyr::filter(View_solar == "TimeSeries1_v02") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_solar"))
  masterdata_solar_meteo <- masterdata_solar %>% dplyr::filter(View_solar == "Meteologica_TimeSeries") %>% dplyr::mutate(PriceArea = paste0(PriceArea, "_solar"))
  
  # Set all colnames to the same
  colnames(masterdata_demand_ts1)[2:3] <- c("CurveID", "View")
  colnames(masterdata_wind_ts1)[2:3] <- c("CurveID", "View")
  colnames(masterdata_solar_ts1)[2:3] <- c("CurveID", "View")
  colnames(masterdata_demand_meteo)[2:3] <- c("CurveID", "View")
  colnames(masterdata_wind_meteo)[2:3] <- c("CurveID", "View")
  colnames(masterdata_solar_meteo)[2:3] <- c("CurveID", "View")
  
  masterdata_ts1 <- dplyr::bind_rows(masterdata_demand_ts1, masterdata_wind_ts1, masterdata_solar_ts1)
  masterdata_meteo <- dplyr::bind_rows(masterdata_demand_meteo, masterdata_wind_meteo, masterdata_solar_meteo)
  
  allmasterdata <- dplyr::bind_rows(masterdata_ts1, masterdata_meteo) %>% dplyr::mutate(CurveID = as.integer(CurveID))
  
  # Load data from meteologica view and timeseries01_v02
  querypathmeteo <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/SQL/fundamentaldata_flow_meteo.sql"
  system.time(
    getDatameteo <- sqlGetQueryReultsInCo("InCo", 
                                          querypathmeteo, 
                                          startDate = startDateTime - lubridate::days(n),
                                          endDate = endDateTime,
                                          forecastDate = forecastDateTime,
                                          username = "sa_ForwardPowerTrading", 
                                          password = "sVXTFQrQS3p8",
                                          author = "EKN",
                                          team = "LT",
                                          solution = "getfundamentalsEPSIflows_meteo"))
  
  getDatameteo <- getDatameteo %>% 
    dplyr::transmute(ValueDateUTC = ValueDateTimeUTC,
                     Value = Value,
                     CurveId = CurveId)
  
  querypathts1 <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/SQL/fundamentaldata_flow_ts1.sql"
  system.time(
    getDatats1 <- sqlGetQueryReultsInCo("InCo", 
                                        querypathts1,
                                        startDate = startDateTime - lubridate::days(n),
                                        endDate = endDateTime,
                                        forecastDate = forecastDateTime,
                                        username = "sa_ForwardPowerTrading", 
                                        password = "sVXTFQrQS3p8",
                                        author = "EKN",
                                        team = "LT",
                                        solution = "getfundamentalsEPSIflows_ts1"))
  
  # Union loaded data and make small manipulations.
  getData <- dplyr::bind_rows(getDatameteo,getDatats1) %>% 
    dplyr::left_join(allmasterdata, by = c("CurveId" = "CurveID")) %>% 
    dplyr::mutate(PriceArea = stringr::str_replace(PriceArea, "IT_", "")) %>% 
    tidyr::separate(PriceArea, c("Area", "Type"), sep = "_") %>% 
    dplyr::select(ValueDateUTC, Value, Area, Type) %>% 
    dplyr::filter(Area %in% c("aNOR","CNOR"))
  
  allfundamentals <- getData %>% 
    dplyr::mutate(ValueDateCET = lubridate::with_tz(ValueDateUTC, "Europe/Paris"),
                  ValueDateDay = lubridate::floor_date(ValueDateCET, "day")
    ) %>% 
    dplyr::transmute(ValueDate = ValueDateUTC,
                     Curvename = paste0(Area, "_", Type),
                     Value = Value) %>% 
    tidyr::spread(Curvename, Value) 
  
  # Set all na in the first week of data to zero.
  allfundamentals[1:(24*7),][is.na(allfundamentals[1:(24*7),])] <- 0
  
  # Create all valuedates in hourly granularity from start to end date.
  datetimehour <- data.frame(ValueDate = seq(startDateTime - lubridate::days(n), endDateTime-lubridate::minutes(1), "hour"))
  
  # Replace NA's with previous comparable value.
  alldatedata <- datetimehour %>% 
    dplyr::left_join(allfundamentals, by = c("ValueDate" = "ValueDate")) %>% 
    dplyr::mutate(wd = lubridate::wday(lubridate::with_tz(ValueDate, "Europe/Paris")),
                  wOrwk = dplyr::if_else(wd %in% c(7,1), "weekend", "weekday")) %>% 
    tidyr::gather(Curvename, Value, -ValueDate, -wd, -wOrwk) %>%
    dplyr::group_by(wOrwk, hh = lubridate::hour(ValueDate), Curvename) %>% 
    dplyr::mutate(Value = zoo::na.locf(Value)) %>% 
    dplyr::ungroup() %>% 
    tidyr::separate(Curvename, c("Area", "Type"), sep = "_") %>% 
    select(ValueDate, Value, Area, Type)
  
  # Calculate residual load
  residualLoad <- alldatedata %>%
    dplyr::mutate(Value = dplyr::if_else(Type == "demand", Value, -Value)) %>%
    dplyr::group_by(ValueDate, Area) %>%
    dplyr::summarise(Value = sum(Value)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(Type = "residualload")
  
  # Set residual load data together with the rest of the fundemental data
  allfundamentaldata <- dplyr::bind_rows(alldatedata,residualLoad) %>% 
    dplyr::mutate(ValueDateCET = lubridate::with_tz(ValueDate, "Europe/Paris"),
                  ValueDateDay = lubridate::floor_date(ValueDateCET, "day")
    )
  
  #################################### 
  
  #################################### GET ACTUAL DA FLOWS ################################################################
  
  datahere <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/masterdata/"
  filenamemasterdata <- "masterdata_DAflows.csv"
  
  # Load masterdata for DA schedule flows
  masterdata <- read.csv2(paste0(datahere,filenamemasterdata), stringsAsFactors = F)
  
  # Manipulate masterdata
  inputdata <- masterdata %>% 
    dplyr::filter(Area1.Area2 != "query") %>% 
    dplyr::filter(Area1 != "HU" | Area2 != "RO") %>% 
    dplyr::transmute(CurveName1 = paste0(Area1, "_", Area2),
                     CurveName2 = paste0(Area2, "_", Area1),
                     CurveID1 = Area1.Area2,
                     CurveID2 = Area2.Area1)
  
  list1 <- c("NOR")
  list2 <- c("aNOR")
  
  CurveName1 <- inputdata$CurveName1
  for(i in seq_along(list1)) CurveName1 <- gsub(list1[i], list2[i], CurveName1, fixed = TRUE)
  
  CurveName2 <- inputdata$CurveName2
  for(i in seq_along(list1)) CurveName2 <- gsub(list1[i], list2[i], CurveName2, fixed = TRUE)
  
  inputdata$CurveName1 <- CurveName1
  inputdata$CurveName2 <- CurveName2
  
  curveids <- c(inputdata$CurveID1, inputdata$CurveID2)
  curvenames <- c(inputdata$CurveName1, inputdata$CurveName2)
  curvetypes <- idtypes <- rep("a", length(curveids))
  
  # Import data
  getData1 <- get_InCo_data(author="EKN",
                            team="LT",
                            platform= "R",
                            solution = "Demand to EPSI",
                            tableStr="TimeSeries1_v02",
                            ForecastDateStr="ForecastDateUTC", 
                            ValueDateStr="ValueDateUTC", 
                            username ="sa_ForwardPowerTrading" ,
                            password = "sVXTFQrQS3p8", 
                            ids = curveids,
                            names =  curvenames,
                            start = startDateTime - lubridate::days(n),
                            end = lubridate::with_tz(lubridate::floor_date(lubridate::with_tz(forecastDateTime,"Europe/Paris"),"day"),"UTC") - lubridate::minutes(1),
                            forecasts = curvetypes,
                            timeZone = "UTC",
                            tidy = F,
                            endF = lubridate::now())
  
  
  ## Masterdata only for HU<>RO
  #inputdata2 <- masterdata %>% 
  #  dplyr::filter(Area1 == "HU" & Area2 == "RO") %>% 
  #  dplyr::transmute(CurveName1 = paste0(Area1, "_", Area2),
  #                   CurveName2 = paste0(Area2, "_", Area1),
  #                   CurveID1 = Area1.Area2,
  #                   CurveID2 = Area2.Area1)
  #
  #curveids2 <- c(inputdata2$CurveID1, inputdata2$CurveID2)
  #curvenames2 <- c(inputdata2$CurveName1, inputdata2$CurveName2)
  #curvetypes2 <- idtypes <- rep("a", length(curveids2))
  #
  ## Import DA flows for HU<>RO
  #getData2 <- get_InCo_data(author="EKN",
  #                          team="LT",
  #                          platform= "R",
  #                          solution = "Demand to EPSI",
  #                          tableStr="TimeSeries1_v02",
  #                          ForecastDateStr="ForecastDateUTC", 
  #                          ValueDateStr="ValueDateUTC", 
  #                          username ="sa_ForwardPowerTrading" ,
  #                          password = "sVXTFQrQS3p8", 
  #                          ids = curveids2,
  #                          names =  curvenames2,
  #                          start = startDateTime - lubridate::days(n),
  #                          end = lubridate::with_tz(lubridate::floor_date(lubridate::with_tz(forecastDateTime,"Europe/Paris"),"day"),"UTC") - lubridate::minutes(1),
  #                          forecasts = curvetypes2,
  #                          timeZone = "UTC",
  #                          tidy = F,
  #                          endF = lubridate::now())
  #
  ## Manipulate DA flows for HU<>RO
  #getData2 <- getData2 %>% 
  #  dplyr::mutate(ValueDateHour = lubridate::floor_date(ValueDate, "hour")) %>% 
  #  dplyr::group_by(ValueDateHour) %>% 
  #  dplyr::summarise(aHU_RO = mean(HU_RO),
  #                   RO_aHU = mean(RO_HU)) %>% 
  #  dplyr::ungroup()
  
  # Import DA flows for Italy
  querypath <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/SQL/Italy_Internal_DAflows.sql"
  
  getData_it <- sqlGetQueryReultsInCo("InCo", 
                                      querypath, 
                                      startDate = startDateTime - lubridate::days(n),
                                      endDate = lubridate::with_tz(lubridate::floor_date(lubridate::with_tz(forecastDateTime,"Europe/Paris"),"day"),"UTC") - lubridate::minutes(1),
                                      username = "sa_ForwardPowerTrading", 
                                      password = "sVXTFQrQS3p8",
                                      author = "EKN",
                                      team = "LT",
                                      solution = "ITflow_epsi")
  
  # Manipulate DA flows for Italy
  getData_it <- getData_it %>% 
    dplyr::transmute(ValueDate = ValueDateTimeUTC,
                     CNOR_aNOR = Value
    )
  
  # Sat all DA flows together and put it into the right format
  GetData <- getData_it %>% 
    # dplyr::left_join(getData2, by = c("ValueDate" = "ValueDateHour")) %>% 
    #dplyr::left_join(getData_it, by = c("ValueDate" = "ValueDate")) %>% 
    tidyr::gather(name, value, -ValueDate) %>% 
    dplyr::mutate(value = dplyr::if_else(is.na(value), 0.0, value)) %>%
    tidyr::separate(name, c("Area1", "Area2"), sep = "_") %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(name1 = dplyr::if_else(sort(c(Area1,Area2))[1] == Area1,Area1,Area2),
                  name2 = dplyr::if_else(sort(c(Area1,Area2))[1] == Area1,Area2,Area1),
                  Value = dplyr::if_else(Area1 == name1, value, -value)) %>% 
    dplyr::group_by(ValueDate, name1, name2) %>% 
    dplyr::summarise(value = sum(Value)) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(aorf = "Actual",
                  ValueDateCET = lubridate::with_tz(ValueDate, "Europe/Paris"),
                  ValueDateDay = lubridate::floor_date(ValueDateCET, "day")
    )
  
  #################################### 
  
  # Create all days where a DA flow forecast is needed.
  loopdates <- seq(startDateTime, endDateTime-lubridate::minutes(1), "day")
  
  allforecast <- NULL
  
  for(i in(1:length(loopdates))){
    # Find the day in loop
    idate <- lubridate::with_tz(loopdates[i], "Europe/Paris")
    
    # Find the weekday of idate
    iweekday <- lubridate::wday(idate)
    
    # Find the fundemental data for idate
    todayforecast <- allfundamentaldata %>% 
      dplyr::filter(ValueDateDay == idate) %>% 
      tidyr::spread(Type,Value) %>% 
      dplyr::mutate(hh = lubridate::hour(ValueDateCET) + 1) %>% 
      dplyr::select(hh, Area, demand, residualload, solar, wind)
    
    # Set NA's to 0
    todayforecast[is.na(todayforecast)] <- 0
    
    # Find all actual DA flows available for idate
    iActualData <- GetData %>% 
      dplyr::filter(ValueDateDay <= pmin(idate - lubridate::days(1),forecastDateTime) & ValueDateDay > pmin(forecastDateTime - lubridate::days(n),idate - lubridate::days(n))) %>% 
      dplyr::mutate(hh = lubridate::hour(ValueDateCET) + 1)
    
    # Find all fundamental data available for idate
    iFundementalData <- allfundamentaldata %>% 
      dplyr::filter(ValueDateDay <= pmin(idate - lubridate::days(1)) & ValueDateDay > pmin(forecastDateTime - lubridate::days(n),idate - lubridate::days(n))) %>% 
      tidyr::spread(Type,Value)
    
    # Set all NA's to 0
    iFundementalData[is.na(iFundementalData)] <- 0
    
    # Join historical fundamental data and fundamental data for idate on actual DA flow data. Hereafter the errors between fundamental data
    # are calculated to find the best comparison day.
    idatajoin <- iActualData %>% 
      dplyr::left_join(iFundementalData, by = c("ValueDate" = "ValueDate", "name1" = "Area", "ValueDateCET" = "ValueDateCET", "ValueDateDay" = "ValueDateDay")) %>% 
      dplyr::left_join(iFundementalData, by = c("ValueDate" = "ValueDate", "name2" = "Area", "ValueDateCET" = "ValueDateCET", "ValueDateDay" = "ValueDateDay")) %>% 
      dplyr::left_join(todayforecast, by = c("hh" = "hh", "name1" = "Area")) %>% 
      dplyr::left_join(todayforecast, by = c("hh" = "hh", "name2" = "Area")) %>% 
      dplyr::mutate(error_rs1 = abs(residualload.x - residualload.x.x)/demand.x.x,
                    error_d1 = abs(demand.x - demand.x.x)/demand.x.x,
                    error_rs2 = abs(residualload.y - residualload.y.y)/demand.y.y,
                    error_d2 = abs(demand.y - demand.y.y)/demand.y.y,
                    total_error = error_rs1 + error_d1 + error_rs2 + error_d2 + 0.005*lubridate::day(lubridate::days(idate - ValueDateDay))
      ) %>% 
      dplyr::group_by(ValueDateDay, name1, name2) %>% 
      dplyr::summarise(daily_error = mean(total_error)) %>% 
      dplyr::ungroup()
    
    # Find the compare dates for each interconnector
    comparedates <- idatajoin %>% 
      dplyr::group_by(name1,name2) %>% 
      dplyr::mutate(minerror = min(daily_error)) %>% 
      dplyr::ungroup() %>% 
      dplyr::filter(daily_error == minerror)
    
    # Filter the actual DA flows based on above compare dates
    finaldata <- iActualData %>% 
      dplyr::left_join(comparedates, by = c("ValueDateDay" = "ValueDateDay", "name1" = "name1", "name2" = "name2")) %>% 
      dplyr::filter(!(is.na(minerror))) %>% 
      dplyr::transmute(hh = hh,
                       icname = paste0(name1,"_",name2),
                       value = value) %>% 
      tidyr::spread(icname, value) %>% 
      dplyr::mutate(ValueDate = lubridate::with_tz(idate + lubridate::hours(0:23), "UTC")) %>% 
      tidyr::gather(icname,value, -hh, - ValueDate)
    
    # Append all forecast data
    allforecast <- dplyr::bind_rows(allforecast, finaldata)
  }
  
  icname <- allforecast$icname
  for(i in seq_along(list1)) icname <- gsub(list2[i], list1[i], icname, fixed = TRUE)
  allforecast$icname <- icname
  
  # Last data manipulations.
  finalflowdata <- allforecast %>%
    dplyr::mutate(icname = paste0(icname, "_", "Flow")) %>% 
    tidyr::spread(icname, value) %>% 
    dplyr::select(-hh) %>% 
    dplyr::filter(ValueDate >= startDateTimeInput)
  
  return(finalflowdata)
}

df<-get_flowforecast(forecastDateTime, startDateTime, endDateTime) %>%
  dplyr::rename("137610_hour"="NOR_CNOR_Flow")


df<-df[order(df$ValueDate),] 

savepath <- "//VMWINFIL01/INCOMAS/Trading/Forward power/Tools/EPSI/upload/csv/"
filename <- "IT_FlowForecast_upload.csv"

data.table::fwrite(df,paste0(savepath,filename),sep=";",dec = ".", row.names = F, quote = F, dateTimeAs = "write.csv")

uploadpath <- "https://epsi.it-prod.incom.as/upload-csv"

httr::POST(uploadpath, body = httr::upload_file(paste0(savepath,filename)))
