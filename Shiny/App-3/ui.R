ui = function(){
  navbarPage("EPSI performance", id = "epsibtt", theme = bslib::bs_theme(bootswatch = "flatly"),
             tabPanel(title = "Overview",
                      fluidRow(
                        column(2,
                               dateRangeInput(inputId = "dr1", "Date period:",
                                              start = today()-7,
                                              end = today()+2
                               )
                        ),
                        column(2,
                               selectInput("area_input_ft",
                                           "Country/Area:",
                                           c("DE", "FR", "AT", "BE","NL", "CH", "GB"),
                                           selected = "DE"
                               )
                        ),
                        column(2,uiOutput("selectUI_fueltype")
                        ),
                        column(1,actionButton("getdatabutton", "Refresh")
                        )
                      ),
                      fluidRow(column(12,plotly::plotlyOutput("plotgeneration", width = "100%", height = "700"))),
                      fluidRow(column(2,
                                      selectInput("area_input_pr",
                                                  "Country/Area:",
                                                  c("DE", "FR", "AT", "BE","NL", "GB", "ES", "IT", "CH", "HU", "CZ", "SK", "DK1", "DK2", "PL", "SI", "HR", "SE3", "SE4", "FI"),
                                                  selected = "DE"
                                      )
                      )
                      ),
                      fluidRow(column(12,plotly::plotlyOutput("plotprices", width = "100%", height = "700"))),
                      fluidRow(column(6,DT::DTOutput("priceerrorstable", width = "100%")),
                               column(1),
                               column(5,DT::DTOutput("generationerrorstable", width = "100%"))),
                      fluidRow(column(2,
                                      selectInput("errortype",
                                                  "ErrorType:",
                                                  c("hourly_bias", "hourly_mae", "hourly_rmse", "monthly_bias","monthly_mae", "monthly_rmse"),
                                                  selected = "hourly_bias"
                                      )
                      )
                      ),
                      fluidRow(column(6,DT::DTOutput("priceerrorstablehourly", width = "100%")),
                               column(6)),
                      fluidRow(column(12,plotly::plotlyOutput("plotbacktest_perf", width = "100%", height = "700"))),
                      fluidRow(column(12,plotly::plotlyOutput("plotbacktest_perfPeak", width = "100%", height = "700"))),
                      fluidRow(column(12,plotly::plotlyOutput("plotbacktest_perfOP1", width = "100%", height = "700"))),
                      fluidRow(column(5,DT::DTOutput("backtesttable", width = "100%")),
                               column(2),
                               column(5,DT::DTOutput("backtesttableP", width = "100%"))),
                      fluidRow(column(5,DT::DTOutput("backtesttableOP1", width = "100%")),
                               column(7)),
                      fluidRow(column(3,uiOutput("selectUI_icname"))
                      ),
                      fluidRow(column(12,plotly::plotlyOutput("plotflows", width = "100%", height = "700"))),
                      fluidRow(column(5,DT::DTOutput("flowerrorstable", width = "100%")))
             )
  )
} 
