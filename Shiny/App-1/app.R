library(shiny)
library(plotly)
source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_incobisql_query_data.R")
inputStartDateTime<-Sys.time()
inputStartDate <- as.Date(inputStartDateTime)+lubridate::days(1)
inputEndDate <- inputStartDate+lubridate::days(3)

inputsqlpath <- "Shiny/sql/PriceForecastEPSI2.sql"
getData <- sqlGetQueryReultsInCo(databaseName = "InCo", sqlPath = inputsqlpath, StartDate = inputStartDate, EndDate=inputEndDate,  username = "sa_ForwardPowerTrading", password = "sVXTFQrQS3p8", author = "CPE", solution = "ZTP gas price", team = "LT") 
   
  
###Z:\Trading\Forward Power\Tools\EPSI\sql\DApriceforecast.sql

ui <- fluidPage(
  selectInput("PriceArea", label="PriceArea",choices=unique(getData$Area)),
  plotlyOutput("plot"),
  
)

PriceData<-function(){
    source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_incobisql_query_data.R")
    inputStartDateTime<-Sys.time()
    inputStartDate <- as.Date(inputStartDateTime)+lubridate::days(1)
    inputEndDate <- inputStartDate+lubridate::days(3)
    
    inputsqlpath <- "Shiny/sql/PriceForecastEPSI2.sql"
    
    getData<-sqlGetQueryReultsInCo(databaseName = "InCo", sqlPath = inputsqlpath, StartDate = inputStartDate, EndDate=inputEndDate,  username = "sa_ForwardPowerTrading", password = "sVXTFQrQS3p8", author = "CPE", solution = "ZTP gas price", team = "LT") 
    return(getData)
  }

server <- function(input, output) {

  PriceData<-function(){
    source("//srv05/Shared/Analysis\\CodeLibrary\\R\\get_incobisql_query_data.R")
    inputStartDateTime<-Sys.time()
    inputStartDate <- as.Date(inputStartDateTime)+lubridate::days(1)
    inputEndDate <- inputStartDate+lubridate::days(3)
    
    inputsqlpath <- "Shiny/sql/PriceForecastEPSI2.sql"
    
    getData<-sqlGetQueryReultsInCo(databaseName = "InCo", sqlPath = inputsqlpath, StartDate = inputStartDate, EndDate=inputEndDate,  username = "sa_ForwardPowerTrading", password = "sVXTFQrQS3p8", author = "CPE", solution = "ZTP gas price", team = "LT") 
    return(getData)
  }
  loadpricedata <- function() {
    #startDateTime <- lubridate::with_tz(as.POSIXct("2022-06-01 00:00", tz = "Europe/Paris"),"UTC")
    #endDateTime <- lubridate::with_tz(as.POSIXct(as.character(lubridate::today("Europe/Paris") + lubridate::days(2)), tz = "Europe/Paris"),"UTC")
    
    startDateTime <- lubridate::with_tz(as.POSIXct(as.character(as.Date(Sys.time())+lubridate::days(1)), tz = "Europe/Paris"),"UTC")
    endDateTime <- lubridate::with_tz(as.POSIXct(as.character(Sys.time()+lubridate::days(3)), tz = "Europe/Paris"),"UTC")
    
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
    return(actual_data_manipulated)
  }
  
  Pricedata<-reactive({
    PriceData()
})
  
  PricedataActual<-reactive({
    loadpricedata()
  })
    output$plot<-renderPlotly({
  
    plotdata<-Pricedata() %>% 
    dplyr::left_join(loadpricedata,by=c("ValueDate"="ValueDateCET"))
    dplyr::filter(Area==input$PriceArea)
  
    plotdata2<-PricedataActual() %>% 
    dplyr::filter(area==input$PriceArea)
    
    
  plot_ly(plotdata,x=~ValueDateCET, y=c(~Value.x,~Value.y), type="scatter", mode="lines")
  })
}

shinyApp(ui, server)
