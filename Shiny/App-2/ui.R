ui = function(){
  ui <- fluidPage(
    selectInput("PriceArea", label="PriceArea",choices=c("DE", "FR", "AT", "BE","NL", "GB", "ES", "IT", "CH", "HU", "CZ", "SK", "DK1", "DK2", "PL", "SI", "HR", "SE3", "SE4", "FI")),
    plotlyOutput("plot"),
    
  )
} 
