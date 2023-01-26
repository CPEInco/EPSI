rm(list = ls())
source("//srv05/Shared/Analysis/CodeLibrary/R/get_incobisql_data_pub.r")
source("//srv05/Shared/Analysis/CodeLibrary/R/get_incobisql_query_data.r")
library(dplyr)

#countries = c(
#  #"Austria",
#  #"Belgium",  
#  #"Croatia",  
#  #"Czech Republic",  
#  #"Denmark (East)", 
#  #"Denmark (West)",   
#  #"Finland",    
#  #"France",    
#  "Germany",#, 
#  #"Great Britain",   
#  #"Hungary",  
#  #"Italy NORD", 
#  #"Netherlands",    
#  "Poland"   
#  #"Romania",  
#  #"Slovakia",  
#  #"Slovenia",     
#  #"Spain",  
#  #"Sweden Malmo (SE4)", 
#  #"Sweden Stockholm (SE3)", 
#  #"Switzerland")
#)
#time_zone              = "CET"
#startDate              = as.POSIXct("2021-02-01: 00:00:00")
#endDate                = lubridate::now(tz = time_zone)
#latest_forecast_before = 11L

upload_to_db <- function(table_name, data_to_upload){
  databaseName = "InCo"
  username = "sa_ForwardPowerTrading"
  password = "sVXTFQrQS3p8"
  author = "JMP"
  team = "LT"
  solution = "Upload EPSI ARIMA correction"
  
  conn <- DBI::dbConnect(odbc::odbc(),
                         Driver = "ODBC Driver 17 for SQL Server",
                         Server = "inco-bisql-forwardpowertrading.in-commodities.local",
                         Database = databaseName,
                         UID = username,
                         Pwd = password,
                         trustservercertificate = "yes",
                         "connection timeout" = 30,
                         applicationintent = "ReadWrite",
                         App = paste0("Author: ", author,", Team: ", team, ", Platform: R", ", Solution: ", solution))
  
  
  #col_name <- DBI::dbListFields(conn, name = table_name)
  
  DBI::dbWriteTable(conn, name = DBI::Id(schema = "stg", table = table_name), value = data_to_upload, append = TRUE)
  
  DBI::dbDisconnect(conn)
}
EPSI_correction <- function(EPSI_error, h = 1){
  
  num_mdls <- 4*2^5
  
  # The h days to forecast
  forecasted_dates <- seq(max(EPSI_error$ValueDate) + lubridate::hours(1), max(EPSI_error$ValueDate) + lubridate::days(h), by = "hours")
  
  dst_hr_idx <- NULL
  
  if(any(substr(date(forecasted_dates), 6, 10) == '03-27')){
    
    dst_hr_idx <- which(unique(substr(date(forecasted_dates), 6, 10)) == "03-27")
    
  }
  
  # Create weekend dummy variable for training sample and h days ahead
  Weekend_Dummy <- data.frame("ValueDate" = c(EPSI_error$ValueDate, forecasted_dates),
                              "sat" = (lubridate::wday(c(EPSI_error$ValueDate, forecasted_dates), week_start = 1) == 6)+0,
                              "sun" = (lubridate::wday(c(EPSI_error$ValueDate, forecasted_dates), week_start = 1) == 7)+0)
  
  # Output container
  out <- NULL
  
  cat("\nHour:")
  
  for(hr in 0:23){
    cat(" ", hr)
    
    # Filter error data to include only one hour 
    Y <- EPSI_error %>% dplyr::filter(lubridate::hour(ValueDate) == hr) %>% 
      tibble::as_tibble()
    
    # Create training set dummy variables for saturday and sunday
    X <- Weekend_Dummy %>% dplyr::filter(lubridate::hour(ValueDate) == hr, ValueDate %in% Y$ValueDate) %>% 
      dplyr::select(-ValueDate) %>% 
      as.matrix()
    
    # Create dummy variables for the h days forecast for saturday and sunday
    X_new <- Weekend_Dummy %>% dplyr::filter(lubridate::hour(ValueDate) == hr, !(ValueDate %in% Y$ValueDate)) %>% 
      dplyr::select(-ValueDate) %>% 
      as.matrix()
    
    # Three dimensional array for storing results where dim 1 (rows) is time, dim 2 (cols) are the different model specifications and dim 3 are countries
    combination_fcst <- array(NA, dim = c(h, num_mdls, ncol(EPSI_error)-1), dimnames = list(1:h, 1:num_mdls, colnames(EPSI_error)[-1]))
    
    # iteratior for each model specification
    iter_mdl <- 1
    
    # Loop over model specification paramenters
    for(wl in c(60, 120, 240, NA)){
      for(mu in c(T,F)){
        for(p in 0:1){
          for(d in 0:1){
            for(q in 0:1){
              
              
              # Skip white noise and random walks
              if(all(c(p,q) == 0)){
                next
              }
              
              # The case where wl is NA refers to the model with an expanding window forecast
              if(is.na(wl)){
                y <- Y[,-1] # Include all data (i.e. expanding window)
                x <- X
              }else{
                y <- Y[(nrow(Y)- wl):nrow(Y),-1] # Include only data with in specified window
                x <- X[(nrow(X)- wl):nrow(X),]
              }
              
              # Fit model with and without weekend dummies. If an error occurs (e.g. singular covariance matrix) h rows of NA's are returned
              fits_wDummy  <- apply(y, MARGIN = 2, FUN = function(y){
                try({forecast::Arima(y = y, order = c(p,d,q), include.mean = mu, method = "ML", xreg = x)}, silent = T)
              })
              fits_woDummy <- apply(y, MARGIN = 2, FUN = function(y){
                try({forecast::Arima(y = y, order = c(p,d,q), include.mean = mu, method = "ML")}, silent = T)
              })
              
              # Handling of errors and non-convergence in fitting procedure
              fcst_wDummy  <- lapply(fits_wDummy, FUN = function(element){
                if(any(class(element) %in% c("try-error", "character"))){ # If an error occurred in the fitting procedure returns h row of NA's
                  return(rep(NA,h))
                }else if(element$code != 0){ # If the optimization algorithm used to maximize the log-lieklihood function does not converge returns h row of NA's
                  return(rep(NA, h))
                }else{
                  return(forecast::forecast(element, h = h, xreg = X_new)$mean) # Else returns h day ahead forecasts
                }
              })
              
              # Same as above but for models excluding weekend dummies
              fcst_woDummy <- lapply(fits_woDummy, FUN = function(element){
                if(any(class(element) %in% c("try-error", "character"))){
                  return(rep(NA,h))
                }else if(element$code != 0){
                  return(rep(NA, h))
                }else{
                  return(forecast::forecast(element, h = h)$mean)
                }
              })
              
              # Convert results from list to matrix and fill in storage array
              combination_fcst[,iter_mdl,]     <- matrix(unlist(fcst_wDummy), nrow = h)
              combination_fcst[,iter_mdl + 1,] <- matrix(unlist(fcst_woDummy), nrow = h)
              
              iter_mdl <- iter_mdl + 2
              
            }
          }
        }
      }
    }
    
    # When forecasts from all models are generated remove duplicate columns and average over forecasts from different models
    combination_fcst <- combination_fcst %>% apply(MARGIN = 3, function(M){      return(data.frame(M)[!duplicated(as.list(data.frame(M)))])    }) %>% 
      lapply(FUN = Matrix::rowMeans, na.rm = TRUE) %>%
      unlist() %>%
      matrix(nrow = h) %>%
      magrittr::set_colnames(dimnames(combination_fcst)[[3]])
    
    # Bind to output
    out <- rbind(out, 
                 data.frame(data.frame("ValueDate" = forecasted_dates) %>% dplyr::filter(hour(ValueDate) == hr), if(is.null(dst_hr_idx) | hr != 2){combination_fcst}else{combination_fcst[-dst_hr_idx,]}))
    
  }
  
  
  # Return output in long format with relevant columns and order by ValueDate
  return(
    out %>% magrittr::set_colnames(colnames(EPSI_error)) %>% 
      tidyr::pivot_longer(cols = colnames(EPSI_error)[-1], values_to = "Value", names_to = "Country") %>%
      dplyr::arrange(Country, ValueDate) %>%
      dplyr::mutate(ValueDateUTC = lubridate::with_tz(ValueDate, "UTC"), ForecastDateUTC = forecasted_dates[1] - lubridate::hours(20) ) %>%
      dplyr::select(ValueDateUTC, Value, Country, ForecastDateUTC) %>%
      as.data.frame()
  )
  
}
get_historical_EPSI_errors <- function(countries, startDate = as.POSIXct("2021-11-12: 00:00:00"), endDate = lubridate::now(tz = time_zone) , latest_forecast_before = 11L, time_zone = "CET"){
  
  # First data point should be hour 00 and last should be hour 23
  lubridate::hour(endDate)     <- 23 
  lubridate::minute(endDate)   <- 0
  lubridate::second(endDate)   <- 0
  lubridate::hour(startDate)   <- 0 
  lubridate::minute(startDate) <- 0
  lubridate::second(startDate) <- 0
  
  # Finds indices that match countries argument
  indexes <- which(c( "Austria",  "Belgium",  "Croatia",  "Czech Republic",  "Denmark (East)", "Denmark (West)",   "Finland",    "France",    "Germany",  "Great Britain",   "Hungary",  "Italy NORD", "Netherlands",    "Poland",   "Romania",  "Slovakia",  "Slovenia",     "Spain",  "Sweden Malmo (SE4)", "Sweden Stockholm (SE3)", "Switzerland") %in% countries)
  
  # Use indices to select curve ids
  curvenames    <- c( "Austria",  "Belgium",  "Croatia",  "Czech Republic",  "Denmark (East)", "Denmark (West)",   "Finland",    "France",    "Germany",  "Great Britain",   "Hungary",  "Italy NORD", "Netherlands",    "Poland",   "Romania",  "Slovakia",  "Slovenia",     "Spain",  "Sweden Malmo (SE4)", "Sweden Stockholm (SE3)", "Switzerland")[indexes]
  curveids_epsi <- c(1000341700, 1000341701, 1000343027,        1000341704,        1000341709,       1000341708,  1000342868,  1000341699,   1000341698,       1000341707,  1000341703,    1000341712,    1000341702,  1000343031,  1000343033,  1000343037,  1000341711,  1000341705,            1000342870,               1000342869,    1000341706)[indexes]
  curveids_spot <- c( 910002236,  910002193,  910000109,         102720715,         802500643,        802500642,   910000076,   115688036,    910002196,       1000341507,   103250977,     910000082,     910002200,   802500814,   910000097,   910000103,   910000104,   910000105,             910000101,                910000100,     910002194)[indexes]
  
  # Print warning if any strings supplied in countries does not have a matching curve id
  if(!all(countries %in% curvenames)){
    warning({paste("\nNo curve for: '", countries[!countries %in% curvenames], "' Check spelling or add new curve to this function .. ")})
  }
  
  # Execute queries
  EPSI_forecast <- get_InCo_data(ids = curveids_epsi,
                                 names = curvenames,
                                 start = startDate,
                                 end = endDate,
                                 forecasts = rep("d", length(curvenames)),
                                 timeZone = time_zone,
                                 tidy = F,
                                 DA_hour = latest_forecast_before,
                                 platform = "R",
                                 username = "sa_ForwardPowerTrading",
                                 password = "sVXTFQrQS3p8",
                                 solution = "EPSI_MEGA",
                                 author = "JMP",
                                 team = "LT",
                                 tableStr = "TimeSeries1_v02",
                                 ValueDateStr = "ValueDateUTC",
                                 ForecastDateStr = "ForecastDateUTC")
  
  spot_price <- get_InCo_data(ids = curveids_spot,
                              names = curvenames,
                              start = startDate,
                              end = endDate,
                              forecasts = rep("a", length(curvenames)),
                              timeZone = time_zone,
                              tidy = F,
                              DA_hour = latest_forecast_before,
                              platform = "R",
                              username = "sa_ForwardPowerTrading",
                              password = "sVXTFQrQS3p8",
                              solution = "EPSI_MEGA",
                              author = "JMP",
                              team = "LT",
                              tableStr = "TimeSeries1_v02",
                              ValueDateStr = "ValueDateUTC",
                              ForecastDateStr = "ForecastDateUTC")
  
  
  
  if("Great Britain" %in% countries){
    gbpeur <- sqlGetQueryReultsInCo(database  = "InCo", 
                                    sqlPath   = "//VMWINFIL01/INCOMAS/Trading/Forward Power/Employees/JMP/EPSI_autocorrelation_combination/EURGBP.sql",
                                    username  = "sa_ForwardPowerTrading",
                                    password  = "sVXTFQrQS3p8",
                                    author    = "jmp", 
                                    team      = "LT", 
                                    solution  = "EPSI_MEGA",
                                    startDate = startDate,
                                    endDate   = endDate)
    
    gb_in_eur <- NULL
    
    for(i in 1:nrow(gbpeur)){
      
      conv_date <- gbpeur$Date[i]
      conv_val  <- gbpeur$Value[i]
      
      gb_in_eur <- append(gb_in_eur, spot_price %>% dplyr::filter(date(ValueDate) == conv_date) %>% dplyr::mutate(`Great Britain` = `Great Britain`*conv_val) %>% dplyr::select(`Great Britain`) %>% unlist() %>% unname() %>% round(2))
      
    }
    
    spot_price$`Great Britain` <- gb_in_eur
    
  }
  
  spot_price <- spot_price %>% dplyr::filter(ValueDate %in% EPSI_forecast$ValueDate)
  
  # Order country alphabetically
  EPSI_forecast <- EPSI_forecast %>% dplyr::select(order(colnames(EPSI_forecast),decreasing = FALSE)) %>% dplyr::relocate(ValueDate, 1)
  spot_price    <- spot_price %>% dplyr::select(order(colnames(spot_price),decreasing = FALSE)) %>% dplyr::relocate(ValueDate, 1)
  
  # Return in wide format
  return(tibble::tibble("ValueDate" = spot_price$ValueDate, EPSI_forecast[,-1] - spot_price[,-1]))
  
}

# Get historical EPSI errors
EPSI_error <- get_historical_EPSI_errors(countries = c(
  "Austria",
  "Belgium",  
  #"Croatia",  
  "Czech Republic",  
  "Denmark (East)", 
  "Denmark (West)",   
  #"Finland",    
  "France",    
  "Germany", 
  "Great Britain",   
  "Hungary",  
  "Italy NORD", 
  "Netherlands",    
  #"Poland",   
  #"Romania",  
  #"Slovakia",  
  "Slovenia",     
  "Spain",  
  #"Sweden Malmo (SE4)", 
  #"Sweden Stockholm (SE3)", 
  "Switzerland"
), startDate = as.POSIXct("2021-02-24: 00:00:00"), endDate = as.POSIXct("2022-11-14: 23:00:00"))

# Generate forecasts

i <- 1

while(min(EPSI_error$ValueDate) + lubridate::days(250+i) - lubridate::hours(1) <= as.POSIXct("2022-11-14: 23:00:00")){
  
  cat("\n\nForecasting date", paste0(date(min(EPSI_error$ValueDate) + lubridate::days(250+i) - lubridate::hours(1))))
  
  EPSI_error_sub <- EPSI_error %>% dplyr::filter(ValueDate <= min(ValueDate) + lubridate::days(250+i) - lubridate::hours(1))
  
  EPSI_error_forecast <- EPSI_correction(EPSI_error_sub, h = 15)
  
  isDAForecast <- (lubridate::date(with_tz(EPSI_error_forecast$ValueDateUTC, "CET")) == lubridate::date(EPSI_error_sub$ValueDate[nrow(EPSI_error_sub)]) + lubridate::days(1)) + 0
  
  # Join isDAForecast to dataframe 
  if(is.null(EPSI_error_forecast$isDAForecast)){
    EPSI_error_forecast <- cbind(EPSI_error_forecast, isDAForecast)
  }
  
  # Round value column to 2 decimal points
  EPSI_error_forecast$Value <- round(EPSI_error_forecast$Value, digits = 2)
  
  upload_to_db("EPSI_MEGA", EPSI_error_forecast)
  
  i <- i + 1
  
}


