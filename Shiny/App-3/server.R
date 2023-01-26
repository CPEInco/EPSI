server <- function(input, output, session) {
  
  # observe({
  #   updateDateRangeInput(session, "dr1",
  #                        label = "Date range:",
  #                        start = today()-7,
  #                        end = today() + 2
  #   )
  # })
  
  pricedata <- reactive({
    input$getdatabutton
    loadpricedata(input$dr1[1], input$dr1[2])
  })
  
  generationdata <- reactive({
    input$getdatabutton
    loadgenerationdata_actual(input$dr1[1], input$dr1[2], country_input = input$area_input_ft)
  })
  
  flowdata <- reactive({
    input$getdatabutton
    loadflowdata_actual(input$dr1[1], input$dr1[2])
  })
  
  backtestdata <- reactive({
    input$getdatabutton
    backtestperformance(data_input = pricedata())
  })
  
  backtestdataOP1 <- reactive({
    input$getdatabutton
    backtestperformanceOP1(data_input = pricedata())
  })
  
  backtestdataP <- reactive({
    input$getdatabutton
    backtestperformanceP(data_input = pricedata())
  })
  
  output$selectUI_fueltype <- renderUI({
    choices<-unique(generationdata()$FuelType)
    selectInput("selectUI_fueltype", "Fueltype:", choices)
  })
  
  output$selectUI_icname <- renderUI({
    choices<-unique(flowdata()$ic_name)
    selectInput("icname", "Interconnector:", choices)
  })
  
  output$plotprices <- plotly::renderPlotly({
    plotdata <- pricedata()
    plotdata <- plotdata %>% dplyr::filter(area == input$area_input_pr & ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    
    plotly::plot_ly(data = plotdata, x=~ValueDate, y=~Value, color = ~fORa, type = "scatter",mode = 'lines', colors=c('actual' = 'red','EPSI_09' = 'black'))
  })
  
  output$plotgeneration <- plotly::renderPlotly({
    plotdata <- generationdata()
    plotdata <- plotdata %>% dplyr::filter(FuelType == input$selectUI_fueltype & ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    
    plotly::plot_ly(data = plotdata, x=~ValueDate, y=~Value, color = ~fORa, type = "scatter",mode = 'lines', colors=c('red', 'blue'))
  })
  
  output$plotflows<- plotly::renderPlotly({
    plotdata <- flowdata()
    plotdata <- plotdata %>% dplyr::filter(ic_name == input$icname & ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    
    plotly::plot_ly(data = plotdata, x=~ValueDate, y=~Value, color = ~fORa, type = "scatter",mode = 'lines', colors=c('actual' = 'red','EPSI_09' = 'black'))
  })
  
  output$plotbacktest_perf <- plotly::renderPlotly({
    plotdata <- backtestdata()
    
    plotdata <- plotdata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0))) %>% 
      dplyr::select(Product, PeriodStart, CumPnL = TotalPnL)
    
    plotly::plot_ly(data = plotdata, x=~PeriodStart, y=~CumPnL, color = ~Product,
                    type = "scatter",mode = 'lines', colors=c('Austria Baseload' = 'red','Belgium Baseload' = 'blue', 'Czech Baseload' = 'purple', 'France Baseload' = 'black','Germany Baseload' = 'darkgreen','Hungary Baseload' = 'yellow','Netherlands Baseload' = 'gray','Spain Baseload' = 'magenta', 'Swiss Baseload' ='darkorange','UK Baseload' = 'aquamarine')) %>% 
      plotly::layout(title ="PnL (25MW mandate)")
  })
  
  output$plotbacktest_perfOP1 <- plotly::renderPlotly({
    plotdata <- backtestdataOP1()
    
    plotdata <- plotdata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0))) %>% 
      dplyr::select(Product, PeriodStart, CumPnL = TotalPnL)
    
    plotly::plot_ly(data = plotdata, x=~PeriodStart, y=~CumPnL, color = ~Product, type = "scatter",mode = 'lines',
                    colors=c('Germany 0-6' = 'darkgreen','UK 1+2' = 'aquamarine')) %>% 
      plotly::layout(title ="PnL (25MW mandate)")
  })
  
  output$plotbacktest_perfPeak <- plotly::renderPlotly({
    plotdata <- backtestdataP()
    
    plotdata <- plotdata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0))) %>% 
      dplyr::select(Product, PeriodStart, CumPnL = TotalPnL)
    
    plotly::plot_ly(data = plotdata, x=~PeriodStart, y=~CumPnL, color = ~Product, type = "scatter",mode = 'lines',
                    colors=c('France Peaks' = 'black','Germany Peaks' = 'darkgreen','Hungary Peaks' = 'yellow','Netherlands Peaks' = 'gray','UK Peaks' = 'aquamarine')) %>% 
      plotly::layout(title ="PnL (25MW mandate)")
  })
  
  output$priceerrorstable <- DT::renderDT({
    tabledata <- pricedata()
    tabledata <- tabledata %>% dplyr::filter(ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    data <- tabledata %>%
      tidyr::spread(fORa, Value) %>%
      dplyr::filter(!(is.na(EPSI_09))) %>% 
      dplyr::filter(!(is.na(actual))) %>% 
      dplyr::mutate(e = EPSI_09 - actual,
                    ae = abs(e),
                    es = (e)^2,
                    ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                        dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
      ) %>%
      dplyr::group_by(area) %>%
      dplyr::summarise(bias = sum(e)/n(),
                       mae = sum(ae)/n(),
                       rmse = sqrt(sum(es)/n()))
    
    
    
    
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  output$priceerrorstablehourly <- DT::renderDT({
    tabledata <- pricedata()
    tabledata <- tabledata %>% dplyr::filter(ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    
    if(input$errortype == "hourly_bias"){
      data <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = EPSI_09 - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueDateHour = lubridate::hour(ValueDate) + 1) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueDateHour, area, bias) %>%
        tidyr::spread(area, bias)
    } else if(input$errortype == "hourly_mae") {
      data <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = EPSI_09 - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueDateHour = lubridate::hour(ValueDate) + 1) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueDateHour, area, mae) %>%
        tidyr::spread(area, mae)
    } else if(input$errortype == "hourly_rmse"){
      data <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = EPSI_09 - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueDateHour = lubridate::hour(ValueDate) + 1) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueDateHour, area, rmse) %>%
        tidyr::spread(area, rmse)
    } else if(input$errortype == "monthly_bias"){
      data <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = EPSI_09 - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueMonth = lubridate::floor_date(ValueDate, "month")) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueMonth, area, bias) %>%
        tidyr::spread(area, bias)
    } else if(input$errortype == "monthly_mae"){
      data <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = EPSI_09 - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueMonth = lubridate::floor_date(ValueDate, "month")) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueMonth, area, mae) %>%
        tidyr::spread(area, mae)
    } else if(input$errortype == "monthly_rmse"){
      data_bias <- tabledata %>%
        tidyr::spread(fORa, Value) %>%
        dplyr::filter(!(is.na(EPSI_09))) %>%
        dplyr::filter(!(is.na(actual))) %>%
        dplyr::mutate(e = forecast_backtest - actual,
                      ae = abs(e),
                      es = (e)^2,
                      ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                          dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
        ) %>%
        dplyr::group_by(area, ValueMonth = lubridate::floor_date(ValueDate, "month")) %>%
        dplyr::summarise(bias = sum(e)/n(),
                         mae = sum(ae)/n(),
                         rmse = sqrt(sum(es)/n())) %>%
        dplyr::select(ValueMonth, area, rmse) %>%
        tidyr::spread(area, rmse)
    }
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  output$generationerrorstable <- DT::renderDT({
    tabledata <- generationdata()
    tabledata <- tabledata %>% dplyr::filter(ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    data <- tabledata %>%
      tidyr::spread(fORa, Value) %>%
      dplyr::filter(!(is.na(EPSI_09)) & !(is.na(actual))) %>% 
      dplyr::mutate(e = EPSI_09 - actual,
                    ae = abs(e),
                    es = (e)^2,
                    ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                        dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
      ) %>%
      dplyr::group_by(FuelType) %>%
      dplyr::summarise(bias = sum(e)/n(),
                       mae = sum(ae)/n(),
                       rmse = sqrt(sum(es)/n()),
                       mpe = sum(ep)/n()*100)
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 0)
    
  })
  
  output$backtesttable <- DT::renderDT({
    tabledata <- backtestdata()
    
    tabledata <- tabledata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
    
    
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    data <- tabledata %>%
      dplyr::group_by(Area) %>%
      dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                       hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                       pct = sum(abs(trade)/(24*25))/n(),
                       pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                       pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                       pct_neutral = round(sum(trade == 0)/n()*100),
                       possible_trading_days = n())
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  output$backtesttableOP1 <- DT::renderDT({
    tabledata <- backtestdataOP1()
    tabledata <- tabledata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
    
    
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    data <- tabledata %>%
      dplyr::group_by(Area) %>%
      dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                       hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                       pct = sum(abs(trade)/(24*25))/n(),
                       pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                       pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                       pct_neutral = round(sum(trade == 0)/n()*100),
                       possible_trading_days = n())
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  output$backtesttableP <- DT::renderDT({
    tabledata <- backtestdataP()
    
    tabledata <- tabledata %>% 
      dplyr::filter(PeriodStart >= input$dr1[1] & PeriodStart < input$dr1[2])%>% 
      dplyr::group_by(Area) %>% 
      dplyr::mutate(TotalPnL = cumsum(PnL),
                    TotalTrade = cumsum(abs(trade))) %>% 
      dplyr::ungroup() %>% 
      dplyr::mutate(PnLMWh = dplyr::if_else(TotalTrade == 0,0,TotalPnL/TotalTrade),
                    hit = dplyr::if_else(PnL > 0, 1, dplyr::if_else(PnL < 0, -1, 0)))
    
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    data <- tabledata %>%
      dplyr::group_by(Area) %>%
      dplyr::summarise(PnLMWh = sum(PnL)/sum(abs(trade)),
                       hit_rate = sum(hit[hit > 0])/sum(abs(hit)),
                       pct = sum(abs(trade)/(24*25))/n(),
                       pct_short = round(sum(trade == -1*24*25)/n()*100,2),
                       pct_long = round(sum(trade == 1*24*25)/n()*100,2),
                       pct_neutral = round(sum(trade == 0)/n()*100),
                       possible_trading_days = n())
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  output$flowerrorstable <- DT::renderDT({
    tabledata <- flowdata()
    tabledata <- tabledata %>% dplyr::filter(ValueDate >= input$dr1[1] & ValueDate < input$dr1[2])
    #tabledata <- tabledata %>% dplyr::filter(ValueDate >= "2020-08-01" & ValueDate < "2021-03-01" & area != "NO")
    
    
    data <- tabledata %>%
      dplyr::group_by(ValueDate, ValueDateUTC, ic_name, fORa) %>% 
      dplyr::summarise(Value = mean(Value)) %>% 
      dplyr::ungroup() %>% 
      tidyr::spread(fORa, Value) %>%
      dplyr::filter(!(is.na(EPSI_09))) %>% 
      dplyr::filter(!(is.na(actual))) %>% 
      dplyr::mutate(e = EPSI_09 - actual,
                    ae = abs(e),
                    es = (e)^2,
                    ep = dplyr::if_else(actual == 0 & EPSI_09 == 0,0,
                                        dplyr::if_else(actual == 0 & EPSI_09 != 0, 100,abs(e/actual)))
                    # e_live = dplyr::if_else(is.na(forecast_live), NA_real_, forecast_live - actual),
                    # ae_live = dplyr::if_else(is.na(e_live), NA_real_,abs(e_live)),
                    # es_live = dplyr::if_else(is.na(e_live), NA_real_,(e_live)^2),
                    # ep_live = dplyr::if_else(is.na(e_live), NA_real_, dplyr::if_else(actual == 0 & forecast == 0,0,
                    #                                                                  dplyr::if_else(actual == 0 & forecast != 0, 100,abs(e_live/actual))))
      ) %>%
      dplyr::group_by(ic_name) %>%
      dplyr::summarise(bias = sum(e)/n(),
                       mae = sum(ae)/n(),
                       rmse = sqrt(sum(es)/n())) %>% 
      dplyr::arrange(rmse)
    
    DT::datatable(data,
                  extensions = 'FixedColumns', options = list("searching"=F, "ordering"=T, "paging"=F, "info"=F,
                                                              buttons = c('csv', 'excel'),
                                                              columnDefs = list(list(className = 'dt-center cell-border-right', targets = 0:(ncol(data)))
                                                                                ,list(visible=FALSE, targets=c(0))
                                                              ),
                                                              initComplete = DT::JS(
                                                                "function(settings, json) {",
                                                                "$(this.api().table().header()).css({'background-color': '#d4d4d4', 'font-size': '100%'});",
                                                                "}")
                  ),
                  escape = T,
                  class = 'stripe nowrap hover'
    ) %>%
      DT::formatStyle(c(1), backgroundColor = "lightgrey", fontWeight = "bold") %>%
      DT::formatStyle(columns = 1:(ncol(data) - 1), fontSize = '100%') %>%
      DT::formatStyle(columns = names(data)[c(-1)], color = DT::styleInterval(c(-0.01, 0.01), c('red', 'black', 'green'))) %>%
      DT::formatCurrency(colnames(data)[2:ncol(data)],currency = "", interval = 3, mark = ",", digits = 2)
    
  })
  
  
}