
#if(!exists("packload")){
#  source("//172.17.0.6/Shared\\Analysis\\CodeLibrary\\R\\BaseFunctions.R") 
#}
#packload(c("dplyr","tidyr","lubridate","stringr","odbc","data.table","DBI"))
#packload_GH("squr","smbache")

#library("dplyr")
#library("tidyr")
#library("lubridate")
#library("stringr")
#library("data.table")
#library("DBI")
#library("squr")




sqlGetQueryResultsFP <- function(databaseName, sqlPath, username, password, author, team, solution, ...) {
  
 conn <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 17 for SQL Server",
                        Server = "inco-bisql-forwardpowertrading.in-commodities.local",
                        Database = databaseName,
                        UID = username,
                        Pwd = password,
                        trustservercertificate = "yes",
                        "connection timeout" = 60,
                        applicationintent = "ReadOnly",
                        App = paste0("Author: ", author,", Team: ", team, ", Platform: R", ", Solution: ", solution)
 )
  
  

  
  sqlPathEdited <- sq_file(sqlPath)
  
  if(length(list(...))==0){
    sqlPathFinal <- sqlPathEdited
  } else {
    sqlPathFinal <- sq_set(sqlPathEdited, ...)
  }
  
  sqlResult <- DBI::dbGetQuery(conn, statement = as.character(sqlPathFinal))
  
  DBI::dbDisconnect(conn)
  
  return(sqlResult)
}

