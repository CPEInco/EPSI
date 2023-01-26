
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