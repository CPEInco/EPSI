#################################### HEADER ####################################
# Author: EAF
# Started: 2018-08-09
# Last changed: 2018-08-09
# Project: Get lilac data
# Country: multi
# 
# Short description: 
# A function to get timeseries from lilac
# Output: a tibble with the data
################################################################################


#################################### PACKAGES ##################################
#if(!exists("packload")){
#  source("//172.17.0.6/Shared\\Analysis\\CodeLibrary\\R\\BaseFunctions.R") 
#}
#packload(c("dplyr","tidyr","lubridate","stringr","odbc","data.table","DBI"))
#packload_GH("squr","smbache")
################################################################################


#################################### Main Function #############################
get_InCo_data <- function(author,team,platform,solution,ids,username,password, names=NULL, start = format(Sys.Date()-1,format = "%Y-%m-%d"), end = format(Sys.Date(),format = "%Y-%m-%d"), timeZone = "UTC", forecasts = c("a","l","d","l1","f")[2], DA_days = -1L, DA_hour = 12L, hours_add = 0L, latest = F, tidy = F, CurveIdStr = "CurveId", ForecastDateStr="ForecastDateTimeUTC", ValueDateStr="ValueDateTimeUTC", tableStr="Meteologica_TimeSeries", startF = NULL, endF = NULL, ForecastDate=F) {
  # author = "efr";team = "Analysis";platform = "R";solution = ""
  # ids = 1000100030;names="NIWind";start=Sys.Date();end=Sys.Date();timeZone = "UTC";forecasts="l";DA_days=-1L;DA_hour=12L;hours_add=0L;latest=F;tidy=F;UID="itservice@in-commodities.com";Pwd="itservice@298";ForecastDate=F
  # CurveIdStr = "CurveId";ForecastDateStr="ForecastDateTimeUTC";ValueDateStr="ValueDateTimeUTC";tableStr="Meteologica_TimeSeries"
  Database = "InCo"
  sqlpath_latest <- "CodeLibrary/SQL Functions/latest_FC.sql"
  sqlpath_first <- "CodeLibrary/SQL Functions/first_FC.sql"
  sqlpath_latest1 <- "CodeLibrary/SQL Functions/latest1_FC.sql"
  sqlpath_dah <- "CodeLibrary/SQL Functions/dah_FC.sql"
  sqlpath_a <- "CodeLibrary/SQL Functions/simple.sql"
  ids <- as.integer(ids)
  start <- as.character(start);end <- as.character(end)
  if(is.null(startF)) startF <- as.Date(start)-14
  if(is.null(endF)) endF <- as.Date(end)+14
  #convert start and end to dateformat
  spectime <- F
  if(timeZone!="UTC"){
    if(nchar(start)<11) start <- format(as.Date(start,format="%Y-%m-%d")-1,"%Y-%m-%d 20:00") else spectime <- T
    if(nchar(end)<11) end <- format(as.Date(end,format="%Y-%m-%d"),"%Y-%m-%d 23:59")
  } else{
    if(nchar(start)<11) start <- format(as.Date(start,format="%Y-%m-%d"),"%Y-%m-%d 00:00")
    if(nchar(end)<11) end <- format(as.Date(end,format="%Y-%m-%d"),"%Y-%m-%d 23:59")
  }
  idstring <- paste0("(",paste0(ids, collapse = ","),")")
  
  con <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 17 for SQL Server",
                        Server = "inco-bisql.in-commodities.local",
                        Database = Database,
                        UID = username,
                        Pwd = password,
                        trustservercertificate = "yes",
                        "connection timeout" = 240,
                        applicationintent = "ReadOnly",
                        App = paste0("Author: ",author,", Team: ", team, ", Platform: ", platform, ", Solution: ", solution))
  
  if(length(forecasts) == length(ids)){
    Aids <- ids[forecasts=="a"];Aidstring <- paste0("(",paste0(Aids, collapse = ","),")")
    Lids <- ids[forecasts=="l"];Lidstring <- paste0("(",paste0(Lids, collapse = ","),")") 
    Fids <- ids[forecasts=="f"];Fidstring <- paste0("(",paste0(Fids, collapse = ","),")") 
    L1ids <- ids[forecasts=="l1"];L1idstring <- paste0("(",paste0(L1ids, collapse = ","),")")
    Dids <- ids[forecasts=="d"];Didstring <- paste0("(",paste0(Dids, collapse = ","),")") 
    ids <- paste0(ids, "_", forecasts)
  } else{
    stop("'forecasts' and 'ids' must be of same length")
  }
  df <- list()
  if(length(Aids)>0){
    sqlPathEdited <- sq_file(sqlpath_a)
    sqlPathFinal <- sq_set(sqlPathEdited,start = start, end = end, startF = startF, endF = endF) %>% 
      stringr::str_replace_all(., pattern = "@ids", replacement = Aidstring) %>% 
      stringr::str_replace_all(., pattern = "@CurveId", replacement = CurveIdStr) %>% 
      stringr::str_replace_all(., pattern = "@ForecastDate", replacement = ForecastDateStr) %>% 
      stringr::str_replace_all(., pattern = "@ValueDate", replacement = ValueDateStr) %>% 
      stringr::str_replace_all(., pattern = "@table", replacement = tableStr)
    # df[["A"]] <- RODBC::sqlQuery(con, query = sqlPathFinal)
    df[["A"]] <- DBI::dbGetQuery(con, statement = sqlPathFinal)
    try({df[["A"]][[1]] <- paste0(df[["A"]][[1]],"_a")}, silent = T)
  }
  if(length(Lids)>0){
    sqlPathEdited <- sq_file(sqlpath_latest)
    sqlPathFinal <- sq_set(sqlPathEdited,start = start, end = end, hours_add = hours_add, startF = startF, endF = endF) %>% 
      stringr::str_replace_all(., pattern = "@ids", replacement = Lidstring) %>% 
      stringr::str_replace_all(., pattern = "@CurveId", replacement = CurveIdStr) %>% 
      stringr::str_replace_all(., pattern = "@ForecastDate", replacement = ForecastDateStr) %>% 
      stringr::str_replace_all(., pattern = "@ValueDate", replacement = ValueDateStr) %>% 
      stringr::str_replace_all(., pattern = "@table", replacement = tableStr)
    if(latest) sqlPathFinal <- sqlPathFinal %>% gsub(x = .,pattern = paste0(' AND (',ForecastDateStr,' <= DATEADD("HOUR", ',hours_add,', ',ValueDateStr,'))'), replacement = "", fixed = T)
    # df[["L"]] <- RODBC::sqlQuery(con, query = sqlPathFinal)
    df[["L"]] <- DBI::dbGetQuery(con, statement = sqlPathFinal)
    try({df[["L"]][[1]] <- paste0(df[["L"]][[1]],"_l")}, silent = T)
  }
  if(length(Fids)>0){
    sqlPathEdited <- sq_file(sqlpath_first)
    sqlPathFinal <- sq_set(sqlPathEdited,start = start, end = end, hours_add = hours_add, startF = startF, endF = endF) %>% 
      stringr::str_replace_all(., pattern = "@ids", replacement = Fidstring) %>% 
      stringr::str_replace_all(., pattern = "@CurveId", replacement = CurveIdStr) %>% 
      stringr::str_replace_all(., pattern = "@ForecastDate", replacement = ForecastDateStr) %>% 
      stringr::str_replace_all(., pattern = "@ValueDate", replacement = ValueDateStr) %>% 
      stringr::str_replace_all(., pattern = "@table", replacement = tableStr)
    if(latest) sqlPathFinal <- sqlPathFinal %>% gsub(x = .,pattern = paste0(' AND (',ForecastDateStr,' <= DATEADD("HOUR", ',hours_add,', ',ValueDateStr,'))'), replacement = "", fixed = T)
    # df[["L"]] <- RODBC::sqlQuery(con, query = sqlPathFinal)
    df[["F"]] <- DBI::dbGetQuery(con, statement = sqlPathFinal)
    try({df[["F"]][[1]] <- paste0(df[["F"]][[1]],"_f")}, silent = T)
  }
  if(length(L1ids)>0){
    sqlPathEdited <- sq_file(sqlpath_latest1)
    sqlPathFinal <- sq_set(sqlPathEdited,start = start, end = end, hours_add = hours_add, startF = startF, endF = endF) %>% 
      stringr::str_replace_all(., pattern = "@ids", replacement = L1idstring) %>% 
      stringr::str_replace_all(., pattern = "@CurveId", replacement = CurveIdStr) %>% 
      stringr::str_replace_all(., pattern = "@ForecastDate", replacement = ForecastDateStr) %>% 
      stringr::str_replace_all(., pattern = "@ValueDate", replacement = ValueDateStr) %>% 
      stringr::str_replace_all(., pattern = "@table", replacement = tableStr)
    if(latest) sqlPathFinal <- sqlPathFinal %>% gsub(x = .,pattern = paste0(' AND (',ForecastDateStr,' <= DATEADD("HOUR", ',hours_add,', ',ValueDateStr,'))'), replacement = "", fixed = T)
    df[["L1"]] <- DBI::dbGetQuery(con, statement = sqlPathFinal)
    try({df[["L1"]][[1]] <- paste0(df[["L1"]][[1]],"_l1")}, silent = T)
  }
  if(length(Dids)>0){
    sqlPathEdited <- sq_file(sqlpath_dah)
    sqlPathFinal <- sq_set(sqlPathEdited,start = start, end = end, DA_days = DA_days, DA_hour = DA_hour, startF = startF, endF = endF) %>% 
      stringr::str_replace_all(., pattern = "@ids", replacement = Didstring) %>% 
      stringr::str_replace_all(., pattern = "@CurveId", replacement = CurveIdStr) %>% 
      stringr::str_replace_all(., pattern = "@ForecastDate", replacement = ForecastDateStr) %>% 
      stringr::str_replace_all(., pattern = "@ValueDate", replacement = ValueDateStr) %>% 
      stringr::str_replace_all(., pattern = "@table", replacement = tableStr)
    df[["D"]] <- DBI::dbGetQuery(con, statement = sqlPathFinal)
    try({df[["D"]][[1]] <- paste0(df[["D"]][[1]],"_d")}, silent = T)
  }
  df <- df %>% dplyr::bind_rows()
  # RODBC::odbcClose(con)
  DBI::dbDisconnect(con)
  colnames(df) <- c("CurveID","ValueDate","ForecastDate","Value")
  if(!tidy){
    df <- df %>% tibble::as_tibble() %>% dplyr::arrange(desc(ForecastDate)) %>% 
      dplyr::select(CurveID, ValueDate, Value) %>% distinct(CurveID, ValueDate, .keep_all = TRUE)
    df <- df %>% tidyr::spread(key = CurveID, value = Value) %>% dplyr::arrange(ValueDate)
  } else{
    df <- df %>% tibble::as_tibble() %>% dplyr::arrange(desc(ForecastDate)) %>% 
      dplyr::select(CurveID, ForecastDate, ValueDate, Value) %>% distinct(CurveID, ValueDate, .keep_all = TRUE)
    if(!ForecastDate) df$ForecastDate <- NULL
  }
  
  if(timeZone != "UTC"){
    df <- df %>% dplyr::mutate(ValueDate = lubridate::with_tz(time = ValueDate, tzone =  timeZone))
    if(!spectime){
      df <- df %>% 
        dplyr::filter(ValueDate >= as.Date(start) + 1, ValueDate <= as.POSIXct(end, tz = timeZone)) 
    }
  }
  if(!tidy){
    na.cols <- as.character(ids[which(!sapply(ids, function(x) any(grepl(pattern = x, x = colnames(df)))))])
    if(length(na.cols)>0) df <- lapply(na.cols, function(x) dplyr::data_frame(y = rep(as.numeric(NA),nrow(df))) %>% data.table::setnames(.,"y",x)) %>% dplyr::bind_cols(.) %>% dplyr::bind_cols(df,.)
    if(!is.null(names)){
      for(i in 1:length(names)){
        colnames(df) <- stringr::str_replace(string = colnames(df), pattern = c(as.character(ids[i])), replacement = names[i])
      }
    }
  } else{
    if(!is.null(names)){
      df$CurveName <- df$CurveID
      for(i in 1:length(names)){
        df$CurveName <- stringr::str_replace(string = df$CurveName, pattern = c(as.character(ids[i])), replacement = names[i])
      }
      df$CurveID <- NULL
    }
  }
  return(df)
}
################################################################################


#################################### Test ######################################
# system.time({
#   test <- get_InCo_data(author = "EFR", team = "Analysis", platform = "R", solution = "Test", ids = 1000100030, names = "MLwind_NI", start = Sys.Date()-10, end = Sys.Date(), timeZone = "CET", forecasts = "l")
# })
################################################################################

# END OF FILE ------------------------------------------------------------------