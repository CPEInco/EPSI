token <- "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"
req <- httr::content(httr::GET("https://api.github.com/repos/incomas/LT.CodeLibrary/contents/R/load_get_data_functions.R", httr::authenticate(token, "")))
devtools::source_url(req$download_url)
rm(req, token)
load_get_data_sql_functions("get_inco_data_local","get_inco_query_data_local")

loadpricedata <- function(startDateTime, endDateTime) {
  #startDateTime <- lubridate::with_tz(as.POSIXct("2022-06-01 00:00", tz = "Europe/Paris"),"UTC")
  #endDateTime <- lubridate::with_tz(as.POSIXct(as.character(lubridate::today("Europe/Paris") + lubridate::days(2)), tz = "Europe/Paris"),"UTC")
  
  startDateTime <- lubridate::with_tz(as.POSIXct(as.character(startDateTime), tz = "Europe/Paris"),"UTC")
  endDateTime <- lubridate::with_tz(as.POSIXct(as.character(endDateTime), tz = "Europe/Paris"),"UTC")
  
  curveids <- c(910002196, 910002197,1000343645, 910002194,910002193, 910002236,910002200, 102720715, 
                1000343776, 910001257, 910001258, 910001254, 910001255, 910000105, 910000104, 910002211, 1000341515, 104157915, 802500814, 910000109, 910001256)
  idnames <- c("DE","FR", "GB", "CH","BE","AT","NL", "CZ","HU", "DK1", "DK2", "SE3", "SE4","ES","SI","IT", "SK", "RO", "PL", "HR", "FI")
  idtypes <- c("a","a","a","a","a","a","a","a","a","a","a","a","a","a","a","a", "a", "a", "a", "a", "a")
  
  actual_data <- get_InCo_data(ids = curveids,
                               names = idnames,
                               start = startDateTime,
                               end = endDateTime,
                               startF = startDateTime - lubridate::days(200),
                               endF = endDateTime,
                               forecasts = idtypes,
                               timeZone = "UTC",
                               tidy = T,
                               platform = "R",
                               username = "sa_ForwardPowerTrading",
                               password = "sVXTFQrQS3p8",
                               solution = "EPSI performance tool",
                               author = "EKN",
                               team = "LT",
                               tableStr = "TimeSeries1_v02",
                               ValueDateStr = "ValueDateUTC",
                               ForecastDateStr = "ForecastDateUTC")
  
  actual_data_manipulated <- actual_data %>% 
    dplyr::transmute(ValueDateUTC = ValueDate,
                     ValueDate = lubridate::with_tz(ValueDateUTC, "Europe/Paris"),
                     Value = dplyr::if_else(CurveName == "PL", round(Value*0.2218467,2), Value),
                     area = CurveName,
                     fORa = "actual"
    ) %>% 
    dplyr::select(ValueDate, ValueDateUTC, Value, area, fORa) %>% 
    dplyr::arrange(ValueDate)
  
  curveids_live <- c(1000341698, 1000341699, 1000341700, 1000341701, 1000341702, 1000341703, 1000341704, 1000341705, 1000341706,
                     1000341707, 1000341708, 1000341709, 1000341710, 1000341711, 1000341712, 1000342869,1000342870, 1000343027,
                     1000343031, 1000343033, 1000343037,1000342868)
  curvenames_live <- c("DE",       "FR",        "AT",        "BE",       "NL",      "HU",        "CZ",       "ES",       "CH",
                       "GB",        "DK1",     "DK2",     "SE",        "SI",        "IT",      "SE3",      "SE4",       "HR",
                       "PL",        "RO",      "SK",      "FI")
  idtypes_live <- rep("d", length(curveids_live))
  
  system.time(
    forecast_data_live <- get_InCo_data(ids = curveids_live,
                                        names = curvenames_live,
                                        start = startDateTime,
                                        end = endDateTime - lubridate::minutes(1),
                                        forecasts = idtypes_live,
                                        timeZone = "UTC",
                                        tidy = T,
                                        DA_hour = 9,
                                        platform = "R",
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8",
                                        solution = "EPSI performance tool",
                                        author = "EKN",
                                        team = "LT",
                                        tableStr = "TimeSeries1_v02",
                                        ValueDateStr = "ValueDateUTC",
                                        ForecastDateStr = "ForecastDateUTC")
  )
  
  forecast_data_live_mani <- forecast_data_live %>%
    dplyr::mutate(ValueDateHour = lubridate::floor_date(ValueDate, "hour")) %>%
    dplyr::group_by(ValueDateHour, CurveName) %>%
    dplyr::summarise(Value = mean(Value)) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(ValueDate = lubridate::with_tz(ValueDateHour, "Europe/Paris"),
                     ValueDateUTC = ValueDateHour,
                     Value = Value,
                     area = CurveName,
                     fORa = "EPSI_09")
  
  alldata <- dplyr::bind_rows(actual_data_manipulated,forecast_data_live_mani)
  
  return(alldata)
}

datahere <- "masterdata/"
filenamemasterdata <- "GenerationdataMasterdata.csv"
masterdata <- read.csv2(paste0(datahere,filenamemasterdata), stringsAsFactors = F)

loadgenerationdata_actual <- function(startDateTime, endDateTime, country_input = "DE"){
  #startDateTime <- lubridate::with_tz(as.POSIXct("2022-04-20 00:00", tz = "Europe/Paris"),"UTC")
  #endDateTime <- lubridate::with_tz(as.POSIXct(as.character(lubridate::today("Europe/Paris") + lubridate::days(2)), tz = "Europe/Paris"),"UTC")
  
  startDateTime <- lubridate::with_tz(as.POSIXct(as.character(startDateTime), tz = "Europe/Paris"),"UTC")
  endDateTime <- lubridate::with_tz(as.POSIXct(as.character(endDateTime), tz = "Europe/Paris"),"UTC")
  
  if(country_input == "DE"){
    curveids <-   c(1000342840,1000342846,1000342848,1000342852,1000342854,1000342858,1000342860,1000342864,1000342850,1000342866,1000342941)
    curvenames <- c("CCGT","Hydro-PS","Hydro-RoR", "ST-Biomass", "ST-Coal","ST-Lignite","ST-Nuclear","ST-Waste","Solar","Wind","PS-Load")
    curvetype <-  rep("d", length(curveids))
  } else if(country_input == "FR"){
    curveids <-  c(1000342841,1000342847,1000342849,1000342853,1000342855,1000342861,1000342863,1000342851,1000342867,1000342942)
    curvenames <- c("CCGT","Hydro-PS","Hydro-RoR","ST-Biomass","ST-Coal","ST-Nuclear","ST-Oil","Solar","Wind","PS-Load")
    curvetype <- rep("d", length(curveids))
  } else if(country_input == "CH"){
    curveids<-c(1000342888,1000342892,1000342900,1000342916,1000342924,1000342896,1000342940)
    curvenames<-c("Hydro-PS","Hydro-RoR","ST-Biomass","ST-Nuclear","ST-Waste","Solar","PS-Load")
    curvetype<-rep("d", length(curveids))
  } else if(country_input == "AT") {
    curveids<-c(1000342874,1000342886,1000342890,1000342898,1000342894,1000342926,1000342938)
    curvenames<-c("CCGT","Hydro-PS","Hydro-RoR","ST-Biomass","Solar","Wind","PS-Load")
    curvetype<-rep("d", length(curveids))
  } else if(country_input == "BE") {
    curveids<-c(1000342875,1000342887,1000342891,1000342907,1000342899,1000342915,1000342923,1000342895,1000342927,1000342939)
    curvenames<-c("CCGT","Hydro-PS","Hydro-RoR","ST-Gas","ST-Biomass","ST-Nuclear","ST-Waste","Solar","Wind","PS-Load")
    curvetype<-rep("d", length(curveids))
  } else if(country_input == "NL"){
    curveids<-c(1000342877,1000342917,1000342905,1000342897,1000342929)
    curvenames<-c("CCGT","ST-Nuclear","ST-Coal","Solar","Wind")
    curvetype<-rep("d", length(curveids))
  } else if(country_input == "GB"){
    curveids<-c(1000344023,1000344026,1000344027,1000344029,1000344030,1000344033,1000344036)
    curvenames<-c("CCGT","Hydro-PS","Hydro-RoR", "ST-Biomass", "ST-Coal","ST-Nuclear","Wind")
    curvetype<-rep("d", length(curveids))
  }
  
  system.time(
    forecast_data_live <- get_InCo_data(ids = curveids,
                                        names = curvenames,
                                        start = startDateTime,
                                        end = endDateTime,
                                        startF = startDateTime - lubridate::days(200),
                                        endF = endDateTime,
                                        forecasts = curvetype,
                                        timeZone = "UTC",
                                        tidy = T,
                                        DA_hour = 09,
                                        platform = "R",
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8",
                                        solution = "EPSI performance tool",
                                        author = "EKN",
                                        team = "LT",
                                        tableStr = "TimeSeries1_v02",
                                        ValueDateStr = "ValueDateUTC",
                                        ForecastDateStr = "ForecastDateUTC")
  )
  
  masterdata_country <- masterdata %>% dplyr::filter(CountryCode == country_input)
  
  curveids <- masterdata_country$CurveId
  curvenames <- masterdata_country$EPSI.FuelType
  idtypes <- rep("a", length(masterdata_country$EPSI.FuelType))
  
  epsiarea <- unique(masterdata_country$CountryName)
  
  masterdata_epsi <- masterdata_country[!(masterdata_country$FuelType %in% c("RENEWABLE", "WIND-OFFSHORE")),]
  
  masterdata_epsi <- masterdata_epsi %>% dplyr::mutate(FuelType = dplyr::if_else(FuelType == "HYDRO-RESERVOIR", "HYDRO-RUN-OF-RIVER",FuelType))
  
  generationforecast_mani <- forecast_data_live %>% 
    dplyr::mutate(ValueDateUTC = lubridate::force_tz(ValueDate, tz = "UTC"),
                  ValueDate = lubridate::with_tz(ValueDate, tz = "Europe/Paris"),
                  Value = as.numeric(Value)) %>% 
    dplyr::left_join(masterdata_epsi, by = c("CurveName" = "EPSI.FuelType")) %>%
    dplyr::mutate(FuelType = dplyr::if_else(substring(FuelType, 1,4) == "WIND", "WIND",FuelType)) %>% 
    dplyr::group_by(ValueDateUTC, FuelType, CountryCode, ValueDate) %>% 
    dplyr::summarise(Value = sum(Value)) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(ValueDateUTC = lubridate::floor_date(ValueDateUTC, "hour"),
                  ValueDate = lubridate::floor_date(ValueDate, "hour")) %>%
    dplyr::group_by(ValueDateUTC, ValueDate, FuelType, CountryCode) %>% 
    dplyr::summarise(Value = mean(Value)) %>% 
    dplyr::ungroup() %>% 
    dplyr::transmute(ValueDate = ValueDate,
                     ValueDateUTC = ValueDateUTC,
                     Value = Value,
                     FuelType = FuelType,
                     fORa = "EPSI_09")
  
  
  actual_data <- get_InCo_data(ids = curveids,
                               names = curvenames,
                               start = min(generationforecast_mani$ValueDateUTC),
                               end = max(generationforecast_mani$ValueDateUTC) - lubridate::minutes(1),
                               forecasts = idtypes,
                               timeZone = "UTC",
                               tidy = T,
                               DA_hour = 9,
                               platform = "R",
                               username = "sa_ForwardPowerTrading",
                               password = "sVXTFQrQS3p8",
                               solution = "EPSI performance tool",
                               author = "EKN",
                               team = "LT",
                               tableStr = "TimeSeries1_v02",
                               ValueDateStr = "ValueDateUTC",
                               ForecastDateStr = "ForecastDateUTC")
  
  
  actual_data_manipulated <- actual_data %>%
    dplyr::mutate(CurveName = dplyr::if_else(substring(CurveName, 1,4) == "WIND", "WIND",CurveName),
                  CurveName = dplyr::if_else(CurveName == "Engine", "CCGT",CurveName),
                  Value = dplyr::if_else(CurveName == "Hydro-PS-C",-Value,Value),
                  CurveName = dplyr::if_else(CurveName == "Hydro-PS-C", "Hydro-PS",CurveName)) %>%
    dplyr::group_by(ValueDate, CurveName) %>%
    dplyr::summarise(Value = sum(Value), .groups = 'drop') %>%
    dplyr::ungroup() %>%
    dplyr::mutate(ValueDateHour = lubridate::floor_date(ValueDate, "hour")) %>% 
    dplyr::group_by(ValueDateHour, CurveName) %>%
    dplyr::summarise(Value = mean(Value), .groups = 'drop') %>%
    dplyr::ungroup() %>%
    dplyr::left_join(masterdata_epsi, by = c("CurveName" = "EPSI.FuelType")) %>%
    dplyr::group_by(ValueDateHour, FuelType) %>%
    dplyr::summarise(Value = sum(Value), .groups = 'drop') %>%
    dplyr::mutate(Value2 = dplyr::if_else((Value/pmax(dplyr::lag(Value,1),1000) > 0.5 & Value > 50000) | (Value/pmax(dplyr::lag(Value,1),1000) > 3 & Value > 3000), dplyr::lag(Value,1), Value)) %>% 
    dplyr::ungroup() %>%
    dplyr::group_by(FuelType) %>%
    dplyr::mutate(#Value2 = dplyr::lag(Value,n=1,order_by = FuelType)) %>% 
      Value2 = dplyr::if_else(is.na(dplyr::if_else((Value/pmax(dplyr::lag(Value,1),1000) > 0.5 & Value > 50000) | (Value/pmax(dplyr::lag(Value,1),1000) > 3 & Value > 3000), dplyr::lag(Value,1), Value)),
                              Value,
                              dplyr::if_else((Value/pmax(dplyr::lag(Value,1),1000) > 0.5 & Value > 50000) | (Value/pmax(dplyr::lag(Value,1),1000) > 3 & Value > 3000), dplyr::lag(Value,1), Value))) %>% 
    dplyr::ungroup() %>%
    dplyr::transmute(ValueDate = lubridate::with_tz(ValueDateHour, "Europe/Paris"),
                     ValueDateUTC = ValueDateHour,
                     Value = Value2,
                     FuelType = dplyr::if_else(substring(FuelType, 1,4) == "WIND", "WIND",FuelType),
                     fORa = "actual")
  
  alldata <- bind_rows(generationforecast_mani,actual_data_manipulated)
  
  return(alldata)
}

loadflowdata_actual <- function(startDateTime, endDateTime){
  startDateTime <- lubridate::with_tz(as.POSIXct("2022-07-25 00:00", tz = "Europe/Paris"),"UTC")
  endDateTime <- lubridate::with_tz(as.POSIXct(as.character(lubridate::today("Europe/Paris") + lubridate::days(2)), tz = "Europe/Paris"),"UTC")
  
  startDateTime <- lubridate::with_tz(as.POSIXct(as.character(startDateTime), tz = "Europe/Paris"),"UTC")
  endDateTime <- lubridate::with_tz(as.POSIXct(as.character(endDateTime), tz = "Europe/Paris"),"UTC")
  
  curveids <-   c(1500000015            ,1500000016,          1500000017,              1000341873,                1000341874,
                  1500000018            ,1500000019         ,1000341877,     1500000020          ,1000341879,                  1000341871,
                  1500000011,                  1500000012,        1000341863,        1500000013,            1000341860,          1000341865,
                  1500000024,                    1500000014,              1500000025,                1000341883,                      1000341880,
                  1000341881,                       1000341882,                      1500000021,        1000341888,              1000341889,
                  1000341887,            1000341886,            1000341890,                  1500000022,        1500000023,        1000341893,
                  1500000029,              1000341896,              1000341897,           1000341895,              1000341898,                  1000341899,
                  1000341900,                1500000027,      1000341884,                1000341903,                    1000341901,
                  1000341904,                                 1000341902,            1500000028,         1500000026)
  curvenames <- c("Germany_Austria", "Germany_Belgium", "Germany_Czech Republic", "Germany_Denmark (West)", "Germany_Denmark (East)",
                  "Germany_France","Germany_Netherlands","Germany_Norway","Germany_Poland","Germany_Sweden Malmo (SE4)", "Germany_Switzerland",
                  "Austria_Czech Republic","Austria_Hungary","Austria_Italy NORD", "Austria_Slovenia", "Austria_Switzerland","Belgium_Great Britain",
                  "Netherlands_Belgium","Czech Republic_Slovakia" ,"Poland_Czech Republic", "Denmark (East)_Sweden Malmo (SE4)","Denmark (West)_Denmark (East)",
                  "Denmark (West)_Norway","Denmark (West)_Sweden Stockholm (SE3)","France_Belgium", "France_Great Britain", "France_Italy NORD",
                  "France_Spain",  "France_Switzerland", "Great Britain_Ireland incl NI", "Croatia_Hungary","Hungary_Romania", "Hungary_Serbia Montenegro",
                  "Slovakia_Hungary","Italy NORD_Italy CNOR", "Italy NORD_Slovenia","Italy NORD_Switzerland","Netherlands_Denmark (West)" ,"Netherlands_Great Britain",
                  "Netherlands_Norway", "Slovenia_Croatia", "Spain_Portugal","Sweden Stockholm (SE3)_Finland","Sweden Malmo (SE4)_Lithuania",
                  "Sweden Stockholm (SE3)_Norway", "Sweden Malmo (SE4)_Poland",  "Hungary_Slovenia", "Poland_Slovakia")
  curvetype <-  rep("d", length(curveids))
  
  system.time(
    forecast_data_live <- get_InCo_data(ids = curveids,
                                        names = curvenames,
                                        start = startDateTime,
                                        end = endDateTime,
                                        startF = startDateTime - lubridate::days(200),
                                        endF = endDateTime,
                                        forecasts = curvetype,
                                        timeZone = "UTC",
                                        tidy = T,
                                        DA_hour = 9,
                                        platform = "R",
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8",
                                        solution = "EPSI performance tool",
                                        author = "EKN",
                                        team = "LT",
                                        tableStr = "TimeSeries1_v02",
                                        ValueDateStr = "ValueDateUTC",
                                        ForecastDateStr = "ForecastDateUTC")
  )
  
  flowforecast_mani <- forecast_data_live %>% 
    dplyr::mutate(ValueDateUTC = lubridate::force_tz(ValueDate, tz = "UTC"),
                  ValueDate = lubridate::with_tz(ValueDate, tz = "Europe/Paris"),
                  Value = as.numeric(Value),
                  ic_name = CurveName) %>% 
    dplyr::transmute(ValueDate = ValueDate,
                     ValueDateUTC = ValueDateUTC,
                     Value = Value,
                     ic_name = ic_name,
                     fORa = "EPSI_09")
  
  curveids <- c(910000003,910000004,910000718,910000717,910000725,910000726,910000348,910000352,
                910000351,910000346, 910000355, 910000356,910000357,
                910000358,910000359,910000360, 910000361,910000362, 910000363, 910000364,
                910001391,910001394, 910001392,910001400,910001396,910001397,910003581,
                910003582, 910003805, 910003814, 910003804, 910003813, 
                910003808, 910003817, 910003807, 910003816, 910003811, 910003820,
                910003810,  910003819, 910000710, 910000709, 910000011, 910000010,
                910000001, 910000002, 910000703, 910000704, 910000708, 910000707, 910000705, 910000706, 910000008, 910000007, 910000005, 910000006,
                910001971, 910001972, 910001973,910002015 ,910002019 , 910002025
                ,910002527, 910002528, 910002893, 910002897, 910001965, 910001967, 
                1000342996,  1000342994, 1000341713	, 1000341714, 1000342997,1000343001, 910002895, 910002898,910001324,910001325,
                910000012, 910000013, 910002016,910001988)
  idnames <- c("Germany_Austria",  "Austria_Germany",  "Germany_France",  "France_Germany",  "France_Belgium",  "Belgium_France",  "Germany_Netherlands",  "Netherlands_Germany",
               "Netherlands_Belgium",  "Belgium_Netherlands",   "France_Spain",  "Spain_France",  "France_Italy NORD",
               "Italy NORD_France",  "Austria_Italy NORD",  "Italy NORD_Austria",   "Slovenia_Italy NORD",  "Italy NORD_Slovenia",   "Slovenia_Austria",   "Austria_Slovenia",
               "Germany_Denmark (East)", "Denmark (East)_Germany",  "Germany_Sweden Malmo (SE4)", "Sweden Malmo (SE4)_Germany", "Netherlands_Norway", "Norway_Netherlands", "Denmark (West)_Netherlands",
               "Netherlands_Denmark (West)", "Netherlands_Great Britain",   "Great Britain_Netherlands",   "Netherlands_Great Britain-LT","Great Britain_Netherlands-LT", 
               "France_Great Britain",  "Great Britain_France",   "France_Great Britain-LT","Great Britain_France-LT", "Belgium_Great Britain", "Great Britain_Belgium",
               "Belgium_Great Britain-LT", "Great Britain_Belgium-LT", "Switzerland_Italy NORD",  "Italy NORD_Switzerland",   "France_Switzerland",   "Switzerland_France",
               "Switzerland_Germany",   "Germany_Switzerland",   "Austria_Switzerland",   "Switzerland_Austria",   "Czech Republic_Austria",   "Austria_Czech Republic",  
               "Austria_Hungary",   "Hungary_Austria",   "Poland_Germany",   "Germany_Poland",   "Germany_Czech Republic",   "Czech Republic_Germany", 
               "Denmark (West)_Denmark (East)", "Denmark (East)_Denmark (West)", "Denmark (East)_Sweden Malmo (SE4)","Sweden Malmo (SE4)_Denmark (East)" ,"Denmark (West)_Sweden Stockholm (SE3)", "Sweden Stockholm (SE3)_Denmark (West)",
               "Czech Republic_Poland", "Poland_Czech Republic","Czech Republic_Slovakia", "Slovakia_Czech Republic", "Denmark (West)_Norway", "Norway_Denmark (West)",
               "Hungary_Croatia","Croatia_Hungary", "Hungary_Romania", "Romania_Hungary","Hungary_Serbia Montenegro","Serbia Montenegro_Hungary","Hungary_Slovakia","Slovakia_Hungary","Slovenia_Croatia",  "Croatia_Slovenia",
               "Spain_Portugal", "Portugal_Spain", "Sweden Malmo (SE4)_Lithuania", "Lithuania_Sweden Malmo (SE4)")
  idtypes <- rep("l", length(curveids))
  
  #actual_data <- get_lilac_data(ids = curveids, names =  idnames, start = min(flowforecast_mani$ValueDateUTC), end = max(flowforecast_mani$ValueDateUTC) - lubridate::minutes(1), forecasts = idtypes, timeZone = "UTC", UID = "ekn@in-commodities.com", Pwd = "ekn@159", tidy = T)
  
  actual_data <- get_InCo_data(ids = curveids,
                               names = idnames,
                               start = min(flowforecast_mani$ValueDateUTC) - lubridate::days(60),
                               end = max(flowforecast_mani$ValueDateUTC) - lubridate::minutes(1),
                               forecasts = idtypes,
                               timeZone = "UTC",
                               tidy = T,
                               platform = "R",
                               username = "sa_ForwardPowerTrading",
                               password = "sVXTFQrQS3p8",
                               solution = "EPSI performance tool",
                               author = "EKN",
                               team = "LT",
                               tableStr = "TimeSeries1_v02",
                               ValueDateStr = "ValueDateUTC",
                               ForecastDateStr = "ForecastDateUTC")
  
  actual_data1 <- actual_data %>% 
    dplyr::mutate(ValueDate = lubridate::floor_date(ValueDate, "hour")) %>% 
    dplyr::group_by(ValueDate, CurveName) %>% 
    dplyr::summarise(Value = mean(Value)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(CurveName, Value) %>% 
    dplyr::filter(ValueDate >= startDateTime)
  
  
  if(length(actual_data$CurveName[which(actual_data$CurveName == "Lithuania_Sweden Malmo (SE4)")]) == 0) {
    actual_data1$`Lithuania_Sweden Malmo (SE4)` = 0
  }
  
  actual_data1[,2:ncol(actual_data1)] <- actual_data1[,2:ncol(actual_data1)] %>% dplyr::mutate_all(~dplyr::if_else(is.na(.),0,.))
  
  actual_data1 <- actual_data1 %>% 
    tidyr::gather(CurveName, Value, -ValueDate)
  
  
  curveidsic <- c(1000340399, 1000340398, 1000340401, 1000340400,1000341518,1000341522, 1000341519, 1000341521, 1000341520, 1000341523,910004303,  910004296,
                  910004584, 910004585, 910004582, 910004583,
                  910002011, 910001980, 910002012, 910001993
                  ,910000353, 910000354, 1000438738, 1000438742, 1000438746)
  idnamesic <- c("Germany_Belgium", "Belgium_Germany", "Germany_Norway", "Norway_Germany","Czech Republic_Slovakia", "Slovakia_Czech Republic", "Hungary_Romania", 
                 "Romania_Hungary", "Hungary_Slovakia","Slovakia_Hungary", "Sweden Malmo (SE4)_Poland", "Poland_Sweden Malmo (SE4)",
                 "France_Great Britain2",  "Great Britain_France2",   "France_Great Britain-LT2","Great Britain_France-LT2",
                 "Sweden Stockholm (SE3)_Finland", "Finland_Sweden Stockholm (SE3)", "Sweden Stockholm (SE3)_Norway", "Norway_Sweden Stockholm (SE3)",
                 "Denmark (West)_Germany",   "Germany_Denmark (West)", "Hungary_Slovenia", "Poland_Slovakia", "Slovakia_Poland")
  idtypesic <- rep("l", length(curveidsic))
  
  actual_data_ic <- get_InCo_data(ids = curveidsic,
                                  names = idnamesic,
                                  start = min(flowforecast_mani$ValueDateUTC),
                                  end = max(flowforecast_mani$ValueDateUTC) - lubridate::minutes(1),
                                  forecasts = idtypesic,
                                  timeZone = "UTC",
                                  tidy = T,
                                  platform = "R",
                                  username = "sa_ForwardPowerTrading",
                                  password = "sVXTFQrQS3p8",
                                  solution = "EPSI performance tool",
                                  author = "EKN",
                                  team = "LT",
                                  tableStr = "TimeSeries1_v02",
                                  ValueDateStr = "ValueDateUTC",
                                  ForecastDateStr = "ForecastDateUTC")
  
  
  actual_data_mani <- actual_data1 %>% 
    dplyr::select(ValueDate, Value, CurveName) %>% 
    dplyr::bind_rows(actual_data_ic) %>% 
    dplyr::group_by(ValueDate, CurveName) %>% 
    dplyr::summarise(Value = max(Value)) %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(CurveName, Value) %>% 
    dplyr::filter(lubridate::minute(ValueDate) == 0) %>%
    dplyr::transmute(ValueDate = ValueDate,
                     Germany_Austria = Germany_Austria - Austria_Germany,
                     Germany_France = Germany_France - France_Germany,
                     Germany_Netherlands = Germany_Netherlands - Netherlands_Germany,
                     France_Belgium = France_Belgium - Belgium_France,
                     Netherlands_Belgium = Netherlands_Belgium - Belgium_Netherlands,
                     `Germany_Denmark (West)` =  `Germany_Denmark (West)` - `Denmark (West)_Germany`,
                     France_Spain = France_Spain - Spain_France,
                     `France_Italy NORD` = `France_Italy NORD` - `Italy NORD_France`,
                     `Austria_Italy NORD` = `Austria_Italy NORD` - `Italy NORD_Austria`,
                     `Italy NORD_Slovenia` = `Italy NORD_Slovenia` - `Slovenia_Italy NORD`,
                     Austria_Slovenia = Austria_Slovenia - Slovenia_Austria,
                     `Germany_Denmark (East)` = `Germany_Denmark (East)` - `Denmark (East)_Germany`,
                     `Germany_Sweden Malmo (SE4)` = `Germany_Sweden Malmo (SE4)` - `Sweden Malmo (SE4)_Germany`,
                     Netherlands_Norway = Netherlands_Norway - Norway_Netherlands,
                     `Denmark (West)_Norway` = `Denmark (West)_Norway` - `Norway_Denmark (West)`,
                     `Netherlands_Denmark (West)` = `Netherlands_Denmark (West)` - `Denmark (West)_Netherlands`,
                     `Netherlands_Great Britain` = -`Great Britain_Netherlands` - `Great Britain_Netherlands-LT` + (`Netherlands_Great Britain` + `Netherlands_Great Britain-LT`),
                     `Belgium_Great Britain` = -`Great Britain_Belgium` - `Great Britain_Belgium-LT` + (`Belgium_Great Britain` + `Belgium_Great Britain-LT`),
                     `France_Great Britain` = `France_Great Britain` + `France_Great Britain-LT` + `France_Great Britain2` + `France_Great Britain-LT2` - (`Great Britain_France` + `Great Britain_France-LT` + `Great Britain_France2` + `Great Britain_France-LT2`),
                     #`Italy NORD_Switzerland` = `Italy NORD_Switzerland` - `Switzerland_Italy NORD`,
                     France_Switzerland = France_Switzerland - Switzerland_France,
                     Germany_Switzerland = Germany_Switzerland - Switzerland_Germany,
                     Austria_Switzerland = Austria_Switzerland - Switzerland_Austria,
                     `Austria_Czech Republic` = `Austria_Czech Republic` - `Czech Republic_Austria`,
                     Austria_Hungary = Austria_Hungary - Hungary_Austria,
                     Germany_Poland = Germany_Poland - Poland_Germany,
                     `Germany_Czech Republic` = `Germany_Czech Republic` - `Czech Republic_Germany`,
                     `Denmark (West)_Denmark (East)` = `Denmark (West)_Denmark (East)` - `Denmark (East)_Denmark (West)`,
                     `Denmark (East)_Sweden Malmo (SE4)` = `Denmark (East)_Sweden Malmo (SE4)` - `Sweden Malmo (SE4)_Denmark (East)`,
                     `Denmark (West)_Sweden Stockholm (SE3)` = `Denmark (West)_Sweden Stockholm (SE3)` - `Sweden Stockholm (SE3)_Denmark (West)`,
                     Croatia_Hungary = Croatia_Hungary - Hungary_Croatia,
                     Hungary_Romania = Hungary_Romania - Romania_Hungary,
                     `Hungary_Serbia Montenegro` = `Hungary_Serbia Montenegro` - `Serbia Montenegro_Hungary`,
                     Slovakia_Hungary = Slovakia_Hungary - Hungary_Slovakia,
                     Slovenia_Croatia = Slovenia_Croatia - Croatia_Slovenia,
                     Spain_Portugal = Spain_Portugal - Portugal_Spain,
                     `Sweden Malmo (SE4)_Lithuania` = `Sweden Malmo (SE4)_Lithuania` - `Lithuania_Sweden Malmo (SE4)`,
                     `Sweden Stockholm (SE3)_Finland` = `Sweden Stockholm (SE3)_Finland` - `Finland_Sweden Stockholm (SE3)`,
                     Germany_Belgium = Germany_Belgium - Belgium_Germany,
                     Germany_Norway = Germany_Norway - Norway_Germany,
                     `Czech Republic_Slovakia` = `Czech Republic_Slovakia` - `Slovakia_Czech Republic`,
                     `Sweden Malmo (SE4)_Poland` = `Sweden Malmo (SE4)_Poland` - `Poland_Sweden Malmo (SE4)`,
                     #`Italy NORD_Italy CNOR` = `Italy NORD_Italy CNOR` - `Italy CNOR_Italy NORD`,
                     `Poland_Czech Republic` = `Poland_Czech Republic` - `Czech Republic_Poland`,
                     `Sweden Stockholm (SE3)_Norway` = `Sweden Stockholm (SE3)_Norway` - `Norway_Sweden Stockholm (SE3)`,
                     Hungary_Slovenia = Hungary_Slovenia,
                     Poland_Slovakia = Poland_Slovakia - Slovakia_Poland
    ) %>%
    tidyr::gather(ic_name, Value, - ValueDate) %>% 
    dplyr::transmute(ValueDateUTC = ValueDate,
                     ValueDate = lubridate::with_tz(ValueDate, "Europe/Paris"),
                     Value = Value,
                     ic_name = ic_name,
                     fORa = "actual") %>% 
    dplyr::select(ValueDate, ValueDateUTC, Value, ic_name, fORa)
  
  alldata <- bind_rows(actual_data_mani,flowforecast_mani) %>% dplyr::arrange(ValueDate, ValueDateUTC)
  
  return(alldata)
}

backtestperformance <- function(data_input) {
  inputsqlpath <- "sql/BaseloadVWAP.sql"
  
  inputstartDate <- min(data_input$ValueDateUTC[data_input$fORa == "actual"])
  inputendDate <- max(data_input$ValueDateUTC[data_input$fORa == "actual"]) + lubridate::hours(5)
  
  getBaseloadVWAP <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                           sqlPath = inputsqlpath, 
                                           startDate = inputstartDate, 
                                           endDate = inputendDate,
                                           username = "sa_ForwardPowerTrading",
                                           password = "sVXTFQrQS3p8", 
                                           author = "ekn", 
                                           team = "LT", 
                                           solution = "EPSI DA backtest")
  
  getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK Baseload"] <- getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK Baseload"] + lubridate::hours(1)
  getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK Baseload"] <- getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK Baseload"] + lubridate::hours(1)
  
  getBaseloadVWAP$PeriodStart <- lubridate::force_tz(getBaseloadVWAP$PeriodStart, tzone = "Europe/Paris")
  getBaseloadVWAP$PeriodEnd <- lubridate::force_tz(getBaseloadVWAP$PeriodEnd, tzone = "Europe/Paris")
  
  inputsqlpath1 <- "sql/exchagerateGBP.sql"
  
  exchangerate <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                        sqlPath = inputsqlpath1, 
                                        inputdate_1 = inputstartDate, 
                                        inputdate_2 = inputendDate,
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8", 
                                        author = "ekn", 
                                        team = "LT", 
                                        solution = "EPSI DA backtest")
  
  exchangerate$ValueDate <- as.POSIXct(as.character(exchangerate$Date), "Europe/Paris")
  
  dd <- data.frame(ValueDate = lubridate::floor_date(seq(min(getBaseloadVWAP$PeriodStart),max(getBaseloadVWAP$PeriodStart),"day"),"day"))
  
  exchangerates <- dd %>% 
    dplyr::left_join(exchangerate, by = c("ValueDate" = "ValueDate")) %>% 
    dplyr::mutate(GBPEUR = zoo::na.locf(GBPEUR))
  
  getBaseloadVWAP1 <- getBaseloadVWAP %>% 
    dplyr::left_join(exchangerates, by = c("PeriodStart" = "ValueDate")) %>% 
    dplyr::mutate(AveragePrice = dplyr::if_else(Area == "GB", AveragePrice*GBPEUR, AveragePrice))
  
  pricedatamani <- data_input %>% 
    dplyr::mutate(DeliveryDate = lubridate::floor_date(ValueDate, "day")) %>% 
    dplyr::group_by(DeliveryDate, area, fORa) %>% 
    dplyr::mutate(maxprice = max(Value),
                  minprice = min(Value)) %>% 
    dplyr::filter(maxprice < 1000 & minprice > -80) %>% 
    dplyr::summarise(Base_price = mean(Value), .groups = 'drop') %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(fORa, Base_price)
  
  alldata <- getBaseloadVWAP1 %>% 
    dplyr::left_join(pricedatamani, by = c("PeriodStart" = "DeliveryDate", "Area" = "area")) %>% 
    dplyr::filter(!(is.na(actual))) %>% 
    dplyr::filter(!(is.na(EPSI_09))) %>% 
    dplyr::mutate(tradespread = EPSI_09 - AveragePrice,
                  #tradespread = EPSI_09 - AveragePrice,
                  actualspread = actual - AveragePrice,
                  trade = dplyr::if_else(tradespread <= -1, -1*24*25,dplyr::if_else(tradespread >= 1, 1*24*25,0)),
                  PnL = actualspread*trade
    ) %>% 
    dplyr::group_by(Area) %>% 
    dplyr::mutate(TotalPnL = cumsum(PnL),
                  TotalTrade = cumsum(abs(trade))) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                  hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
  
  
  tabledata <- alldata %>%
    dplyr::group_by(Area) %>%
    dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                     hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                     pct = sum(abs(trade)/(24*25))/n(),
                     pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                     pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                     pct_neutral = round(sum(trade == 0)/n()*100))
  
  
  return(alldata)
}

backtestperformanceOP1 <- function(data_input) {
  inputsqlpath <- "sql/Offpeak1VWAP.sql"
  
  inputstartDate <- min(data_input$ValueDateUTC[data_input$fORa == "actual"])
  inputendDate <- max(data_input$ValueDateUTC[data_input$fORa == "actual"]) + lubridate::hours(5)
  
  getBaseloadVWAP <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                           sqlPath = inputsqlpath, 
                                           startDate = inputstartDate, 
                                           endDate = inputendDate,
                                           
                                           username = "sa_ForwardPowerTrading",
                                           password = "sVXTFQrQS3p8", 
                                           author = "ekn", 
                                           team = "LT", 
                                           solution = "EPSI DA backtest")
  
  getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK 1+2"] <- getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK 1+2"] + lubridate::hours(1)
  getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK 1+2"] <- getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK 1+2"] + lubridate::hours(1)
  
  getBaseloadVWAP$PeriodStart <- lubridate::force_tz(getBaseloadVWAP$PeriodStart, tzone = "Europe/Paris")
  getBaseloadVWAP$PeriodEnd <- lubridate::force_tz(getBaseloadVWAP$PeriodEnd, tzone = "Europe/Paris")
  
  inputsqlpath1 <- "sql/exchagerateGBP.sql"
  
  exchangerate <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                        sqlPath = inputsqlpath1, 
                                        inputdate_1 = inputstartDate, 
                                        inputdate_2 = inputendDate,
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8", 
                                        author = "ekn", 
                                        team = "LT", 
                                        solution = "EPSI DA backtest")
  
  exchangerate$ValueDate <- as.POSIXct(as.character(exchangerate$Date), "Europe/Paris")
  
  
  dd <- data.frame(ValueDate = lubridate::floor_date(seq(min(getBaseloadVWAP$PeriodStart),max(getBaseloadVWAP$PeriodStart) + lubridate::days(1),"day"),"day"))
  
  exchangerates <- dd %>% 
    dplyr::left_join(exchangerate, by = c("ValueDate" = "ValueDate"))
  
  if(is.na(exchangerates$GBPEUR[1])) {
    exchangerates$GBPEUR[1] <- exchangerates$GBPEUR[2]
  }
  
  exchangerates <- exchangerates %>% 
    dplyr::mutate(GBPEUR = zoo::na.locf(GBPEUR))
  
  getBaseloadVWAP1 <- getBaseloadVWAP %>% 
    dplyr::left_join(exchangerates, by = c("PeriodStart" = "ValueDate")) %>% 
    dplyr::mutate(AveragePrice = dplyr::if_else(Area == "GB", AveragePrice*GBPEUR, AveragePrice))
  
  pricedatamani <- data_input %>% 
    dplyr::mutate(DeliveryDate = lubridate::floor_date(ValueDate, "day"),
                  hh = lubridate::hour(ValueDate) + 1) %>% 
    dplyr::filter((hh <= 6 & area == "DE") | (hh <= 8 & area == "GB")) %>% 
    dplyr::group_by(DeliveryDate, area, fORa) %>% 
    dplyr::mutate(maxprice = max(Value),
                  minprice = min(Value)) %>% 
    dplyr::filter(maxprice < 1000 & minprice > -80) %>% 
    dplyr::summarise(OP1_price = mean(Value), .groups = 'drop') %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(fORa, OP1_price)
  
  alldata <- getBaseloadVWAP1 %>% 
    dplyr::left_join(pricedatamani, by = c("PeriodStart" = "DeliveryDate", "Area" = "area")) %>% 
    dplyr::filter(!(is.na(actual))) %>% 
    dplyr::filter(!(is.na(EPSI_09))) %>% 
    dplyr::mutate(tradespread = EPSI_09 - AveragePrice,
                  #tradespread = EPSI_09 - AveragePrice,
                  actualspread = actual - AveragePrice,
                  trade = dplyr::if_else(tradespread <= -1, -1*24*25,dplyr::if_else(tradespread >= 1, 1*24*25,0)),
                  PnL = actualspread*trade
    ) %>% 
    dplyr::group_by(Area) %>% 
    dplyr::mutate(TotalPnL = cumsum(PnL),
                  TotalTrade = cumsum(abs(trade))) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                  hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
  
  
  tabledata <- alldata %>%
    dplyr::group_by(Area) %>%
    dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                     hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                     pct = sum(abs(trade)/(24*25))/n(),
                     pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                     pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                     pct_neutral = round(sum(trade == 0)/n()*100))
  
  
  
  return(alldata)
}

backtestperformanceP <- function(data_input) {
  inputsqlpath <- "sql/PeakVWAP.sql"
  
  inputstartDate <- min(data_input$ValueDateUTC[data_input$fORa == "actual"])
  inputendDate <- max(data_input$ValueDateUTC[data_input$fORa == "actual"]) + lubridate::hours(5)
  
  getBaseloadVWAP <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                           sqlPath = inputsqlpath, 
                                           startDate = inputstartDate, 
                                           endDate = inputendDate,
                                           
                                           username = "sa_ForwardPowerTrading",
                                           password = "sVXTFQrQS3p8", 
                                           author = "ekn", 
                                           team = "LT", 
                                           solution = "EPSI DA backtest")
  
  getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK Peaks"] <- getBaseloadVWAP$PeriodStart[getBaseloadVWAP$Product == "UK Peaks"] + lubridate::hours(1)
  getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK Peaks"] <- getBaseloadVWAP$PeriodEnd[getBaseloadVWAP$Product == "UK Peaks"] + lubridate::hours(1)
  
  getBaseloadVWAP$PeriodStart <- lubridate::force_tz(getBaseloadVWAP$PeriodStart, tzone = "Europe/Paris")
  getBaseloadVWAP$PeriodEnd <- lubridate::force_tz(getBaseloadVWAP$PeriodEnd, tzone = "Europe/Paris")
  
  inputsqlpath1 <- "sql/exchagerateGBP.sql"
  
  exchangerate <- sqlGetQueryReultsInCo(databaseName = "InCo", 
                                        sqlPath = inputsqlpath1, 
                                        inputdate_1 = inputstartDate, 
                                        inputdate_2 = inputendDate,
                                        username = "sa_ForwardPowerTrading",
                                        password = "sVXTFQrQS3p8", 
                                        author = "ekn", 
                                        team = "LT", 
                                        solution = "EPSI DA backtest")
  
  exchangerate$ValueDate <- as.POSIXct(as.character(exchangerate$Date), "Europe/Paris")
  
  dd <- data.frame(ValueDate = lubridate::floor_date(seq(min(getBaseloadVWAP$PeriodStart),max(getBaseloadVWAP$PeriodStart),"day"),"day"))
  
  exchangerates <- dd %>% 
    dplyr::left_join(exchangerate, by = c("ValueDate" = "ValueDate"))
  
  if(is.na(exchangerates$GBPEUR[1])) {
    exchangerates$GBPEUR[1] <- exchangerates$GBPEUR[2]
  }
  
  exchangerates <- exchangerates %>% 
    dplyr::mutate(GBPEUR = zoo::na.locf(GBPEUR))
  
  getBaseloadVWAP1 <- getBaseloadVWAP %>% 
    dplyr::left_join(exchangerates, by = c("PeriodStart" = "ValueDate")) %>% 
    dplyr::mutate(AveragePrice = dplyr::if_else(Area == "GB", AveragePrice*GBPEUR, AveragePrice))
  
  pricedatamani <- data_input %>% 
    dplyr::mutate(DeliveryDate = lubridate::floor_date(ValueDate, "day"),
                  hh = lubridate::hour(ValueDate) + 1) %>% 
    dplyr::filter((hh >= 9 & hh < 21)) %>% 
    dplyr::group_by(DeliveryDate, area, fORa) %>% 
    dplyr::mutate(maxprice = max(Value),
                  minprice = min(Value)) %>% 
    dplyr::filter(maxprice < 1000 & minprice > -80) %>% 
    dplyr::summarise(OP1_price = mean(Value), .groups = 'drop') %>% 
    dplyr::ungroup() %>% 
    tidyr::spread(fORa, OP1_price)
  
  alldata <- getBaseloadVWAP1 %>% 
    dplyr::left_join(pricedatamani, by = c("PeriodStart" = "DeliveryDate", "Area" = "area")) %>% 
    dplyr::filter(!(is.na(actual))) %>% 
    dplyr::filter(!(is.na(EPSI_09))) %>% 
    dplyr::mutate(tradespread = EPSI_09 - AveragePrice,
                  #tradespread = EPSI_09 - AveragePrice,
                  actualspread = actual - AveragePrice,
                  trade = dplyr::if_else(tradespread <= -1, -1*24*25,dplyr::if_else(tradespread >= 1, 1*24*25,0)),
                  PnL = actualspread*trade
    ) %>% 
    dplyr::group_by(Area) %>% 
    dplyr::mutate(TotalPnL = cumsum(PnL),
                  TotalTrade = cumsum(abs(trade))) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                  hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
  
  
  tabledata <- alldata %>%
    dplyr::group_by(Area) %>%
    dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                     hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                     pct = sum(abs(trade)/(24*25))/n(),
                     pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                     pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                     pct_neutral = round(sum(trade == 0)/n()*100))
  
  
  
  return(alldata)
}