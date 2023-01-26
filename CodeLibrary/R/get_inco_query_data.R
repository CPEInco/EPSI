
#packload <- function(x){
#  new.packages <- x[!(x %in% installed.packages()[,"Package"])]
#  # if(length(new.packages)) install.packages(new.packages, repos = "http://cran.rstudio.com")
#  lapply(x, require, character.only = T, quietly = T)
#  all_installed <- x[!(x %in% installed.packages()[,"Package"])]
#  if(length(all_installed)) return(paste(paste(all_installed, collapse = ", ")," is not installed for some reason")) else return("package(s) loaded successfully")
#}
#
#packload_GH <- function(x, author){
#  packload("devtools")
#  if(length(x)!=length(author)) stop("x and author must be of the same length")
#  new.packages <- x[!(x %in% installed.packages()[,"Package"])]
#  ax <- paste0(author,"/",x)
#  # if(length(new.packages)) devtools::install_github(ax)
#  lapply(x, require, character.only = T, quietly = T)
#  all_installed <- x[!(x %in% installed.packages()[,"Package"])]
#  if(length(all_installed)) return(paste(paste(all_installed, collapse = ", ")," is not installed for some reason")) else return("package(s) loaded successfully")
#}
#
#packload(c("dplyr","tidyr","lubridate","stringr","odbc","data.table","DBI"))
#packload_GH("squr","smbache")

sqlGetQueryReultsInCo <- function(databaseName, sqlPath, username, password, author, team, solution, ...) {
  
 conn <- DBI::dbConnect(odbc::odbc(),
                        Driver = "ODBC Driver 17 for SQL Server",
                        #ODBC Driver 17 for SQL Server
                        #SQL Server
                        Server = "inco-bisql.in-commodities.local",
                        Database = databaseName,
                        UID = username,
                        Pwd = password,
                        trustservercertificate = "yes",
                        "connection timeout" = 60,
                        applicationintent = "ReadOnly",
                        App = paste0("Author: ", author,", Team: ", team, ", Platform: R", ", Solution: ", solution),
                        encoding = "CP1252"
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

