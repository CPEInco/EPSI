server <- function(input, output, session) {
  
  # observe({
  #   updateDateRangeInput(session, "dr1",
  #                        label = "Date range:",
  #                        start = today()-7,
  #                        end = today() + 2
  #   )
  # })
  
  pricedata <- reactive({
    
    loadpricedata("2022-08-12", "2022-08-13")
  })
  
  output$plot <- plotly::renderPlotly({
    plotdata <- pricedata()
    plotdata <- plotdata %>% dplyr::filter(area == input$PriceArea)
    
    plotly::plot_ly(data = plotdata, x=~ValueDate, y=~Value, color = ~fORa, type = "scatter",mode = 'lines', colors=c('actual' = 'red','EPSI_09' = 'black'))
  })
  
}
